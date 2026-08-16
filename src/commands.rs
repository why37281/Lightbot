//! Tauri 命令层:前端(GUI)调用入口 + 机器人生命周期管理。

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::Value;
use tauri::Manager;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, RwLock};
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;

use crate::chat::ChatCore;
use crate::config::{self, Config, ModelConfig};
use crate::cost::CostTracker;
use crate::events::{EventBuf, FrontendEvent};
use crate::llm::LlmClient;
use crate::memory::MemoryStore;
use crate::napcat::{BotEvent, ConnStatus, NapcatClient};
use crate::placement::PlacementController;
use crate::trace::TraceStore;

// ---------- 应用状态 ----------

pub struct AppState {
    pub cfg_path: PathBuf,
    pub sessions_dir: PathBuf,
    pub config: Arc<RwLock<Config>>,
    /// 前端事件缓冲(拉模式:前端经 get_events 轮询;推送链路不可靠,已弃用)
    pub events: Arc<Mutex<EventBuf>>,
    pub bot: Mutex<Option<BotHandle>>,
    /// 运行中的对话核心(命令层会话操作复用)
    pub chat: Mutex<Option<Arc<ChatCore>>>,
    /// 启动/重启互斥(防并发 start_bot 与 save_config 重启竞态)
    pub restart_lock: tokio::sync::Mutex<()>,
    /// 最近一次连接状态快照(get_status_view 兜底用,不依赖事件链路)
    pub last_status: Arc<Mutex<Option<ConnStatus>>>,
    /// 用量与费用追踪(机器人停止时 GUI 仍可查询)
    pub cost: Arc<Mutex<CostTracker>>,
    /// 记忆位置自动控制状态(机器人停止时审批流程仍可结算)
    pub placement: Arc<Mutex<PlacementController>>,
    /// 全局暂停回复:true 时只接收消息,不决策/不回复(机器人重启后复位)
    pub paused: Arc<AtomicBool>,
}

pub struct BotHandle {
    cancel: CancellationToken,
    tasks: Vec<tauri::async_runtime::JoinHandle<()>>,
    /// NapCat 连接任务(停止时 abort + join,确保端口/资源释放)
    conn_task: tokio::task::JoinHandle<()>,
}

impl BotHandle {
    async fn stop(self) {
        self.cancel.cancel();
        for t in &self.tasks {
            t.abort();
        }
        self.conn_task.abort();
        let _ = self.conn_task.await;
    }
}

// ---------- setup ----------

pub fn setup(app: &mut tauri::App) -> Result<(), Box<dyn std::error::Error>> {
    let cfg_path = config::config_path(app.handle());
    let sessions_dir = config::sessions_dir(app.handle());
    let loaded = config::load_config(&cfg_path);

    // 拉模式事件缓冲:前端定时 get_events,不依赖 Tauri 事件推送
    let events = Arc::new(Mutex::new(EventBuf::new()));

    // 用量目录与配置目录同级(应用数据目录)
    let usage_dir = app
        .handle()
        .path()
        .app_data_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("usage");

    app.manage(AppState {
        cfg_path,
        sessions_dir,
        config: Arc::new(RwLock::new(loaded)),
        events,
        bot: Mutex::new(None),
        chat: Mutex::new(None),
        restart_lock: tokio::sync::Mutex::new(()),
        last_status: Arc::new(Mutex::new(None)),
        cost: Arc::new(Mutex::new(CostTracker::new(usage_dir))),
        placement: Arc::new(Mutex::new(PlacementController::default())),
        paused: Arc::new(AtomicBool::new(false)),
    });
    Ok(())
}

/// 拉取序号大于 after_seq 的前端事件(拉模式事件总线)
#[tauri::command]
pub async fn get_events(
    state: tauri::State<'_, AppState>,
    after_seq: u64,
) -> Result<Value, String> {
    let buf = state.events.lock().map_err(|e| e.to_string())?;
    let (events, latest_seq) = buf.after(after_seq);
    let arr: Vec<Value> = events
        .into_iter()
        .map(|(seq, ev)| serde_json::json!({ "seq": seq, "event": ev }))
        .collect();
    Ok(serde_json::json!({ "events": arr, "latest_seq": latest_seq }))
}

// ---------- 配置命令 ----------

#[tauri::command]
pub async fn get_config(state: tauri::State<'_, AppState>) -> Result<Config, String> {
    Ok(state.config.read().await.clone())
}

#[tauri::command]
pub async fn save_config(
    state: tauri::State<'_, AppState>,
    cfg: Config,
) -> Result<(), String> {
    validate_config(&cfg)?;
    config::save_config(&state.cfg_path, &cfg)?;
    *state.config.write().await = cfg;

    // 运行中则重启机器人,使新配置生效(持重启锁,防与 start_bot 并发)
    let _restart_guard = state.restart_lock.lock().await;
    let old_bot = {
        let mut guard = state.bot.lock().map_err(|e| e.to_string())?;
        guard.take()
    };
    if let Some(b) = old_bot {
        b.stop().await;
        start_bot_inner(&state).await?;
        state.events.lock().unwrap().push(FrontendEvent::Log {
            level: "info".into(),
            msg: "配置已保存,机器人已按新配置重启".into(),
        });
    }
    Ok(())
}

fn validate_config(cfg: &Config) -> Result<(), String> {
    if cfg.models.is_empty() {
        return Err("至少需要一个模型配置".into());
    }
    let mut names: Vec<String> = Vec::new();
    for m in &cfg.models {
        if m.name.trim().is_empty() {
            return Err("模型名称不能为空".into());
        }
        if names.contains(&m.name) {
            return Err(format!("模型名称重复: {}", m.name));
        }
        if !(0.0..=2.0).contains(&m.temperature) {
            return Err("温度需在 0~2 之间".into());
        }
        names.push(m.name.clone());
    }
    if cfg.models.iter().all(|m| m.name != cfg.active_model) {
        return Err("当前激活模型不在模型列表中".into());
    }
    if cfg.prompts.is_empty() {
        return Err("至少需要一个人设".into());
    }
    if cfg.prompts.iter().all(|p| p.id != cfg.active_prompt) {
        return Err("当前激活人设不在列表中".into());
    }
    Ok(())
}

#[tauri::command]
pub fn get_config_path(state: tauri::State<'_, AppState>) -> String {
    state.cfg_path.display().to_string()
}

/// 取运行中的对话核心(未运行返回 None;各命令据此走磁盘兜底)
fn running_chat(state: &tauri::State<'_, AppState>) -> Result<Option<Arc<ChatCore>>, String> {
    Ok(state.chat.lock().map_err(|e| e.to_string())?.clone())
}

// ---------- 机器人控制 ----------

#[tauri::command]
pub async fn start_bot(state: tauri::State<'_, AppState>) -> Result<(), String> {
    let _restart_guard = state.restart_lock.lock().await;
    {
        let guard = state.bot.lock().map_err(|e| e.to_string())?;
        if guard.is_some() {
            return Err("机器人已在运行".into());
        }
    }
    start_bot_inner(&state).await
}

async fn start_bot_inner(state: &tauri::State<'_, AppState>) -> Result<(), String> {
    let cancel = CancellationToken::new();
    let (bot_ev_tx, mut bot_ev_rx) = mpsc::channel::<BotEvent>(512);
    let (status_tx, mut status_rx) = watch::channel(ConnStatus {
        connected: false,
        mode: String::new(),
        endpoint: String::new(),
        self_id: None,
        last_error: String::new(),
    });

    let napcat = Arc::new(NapcatClient::new(
        state.config.clone(),
        bot_ev_tx,
        status_tx,
    )
    .await);
    let (sender, conn_task) = napcat.clone().run(cancel.clone()).await;

    // 决策器全局中止通道(每次启动新建;暂停时发送 true 取消进行中的决策请求)
    let (decide_cancel, _) = watch::channel::<bool>(false);
    state.paused.store(false, Ordering::Relaxed);

    let chat = Arc::new(ChatCore::new(
        state.config.clone(),
        sender,
        state.sessions_dir.clone(),
        state.cfg_path.clone(),
        state.events.clone(),
        state.cost.clone(),
        state.placement.clone(),
        state.paused.clone(),
        decide_cancel,
    ));

    let mut tasks = Vec::new();

    // 事件管线:消息 -> 对话(每消息独立任务:不同会话并行,同会话由串行门保序);
    // 通知/请求 -> 事件缓冲
    let chat2 = chat.clone();
    let events2 = state.events.clone();
    tasks.push(tauri::async_runtime::spawn(async move {
        while let Some(ev) = bot_ev_rx.recv().await {
            match ev {
                BotEvent::Message(m) => {
                    let chat = chat2.clone();
                    tauri::async_runtime::spawn(async move {
                        chat.handle_message(m).await;
                    });
                }
                BotEvent::Notice(n) => {
                    events2.lock().unwrap().push(FrontendEvent::Notice {
                        desc: n.desc,
                        notice_type: n.notice_type,
                    });
                }
                BotEvent::Request(r) => {
                    events2.lock().unwrap().push(FrontendEvent::Notice {
                        desc: format!(
                            "收到{}请求: {} (留言: {})",
                            if r.request_type == "friend" { "好友" } else { "加群" },
                            r.user_id,
                            r.comment
                        ),
                        notice_type: format!("request_{}", r.request_type),
                    });
                }
                BotEvent::Heartbeat => {}
                BotEvent::Lifecycle(l) => {
                    events2.lock().unwrap().push(FrontendEvent::Log {
                        level: "info".into(),
                        msg: format!("连接事件: {l}"),
                    });
                }
            }
        }
    }));

    // 连接状态 -> 事件缓冲(同时写快照供 get_status_view 兜底)
    let events3 = state.events.clone();
    let last_status = state.last_status.clone();
    tasks.push(tauri::async_runtime::spawn(async move {
        while status_rx.changed().await.is_ok() {
            let s = status_rx.borrow().clone();
            *last_status.lock().unwrap() = Some(s.clone());
            events3.lock().unwrap().push(FrontendEvent::Status { status: s.clone() });
            events3.lock().unwrap().push(FrontendEvent::Log {
                level: "info".into(),
                msg: format!(
                    "连接状态: {}{}",
                    if s.connected { "已连接" } else { "未连接" },
                    if s.last_error.is_empty() {
                        String::new()
                    } else {
                        format!(" · {}", s.last_error)
                    }
                ),
            });
        }
    }));

    // 会话空闲清理
    let chat3 = chat.clone();
    let cleaner_cancel = cancel.clone();
    tasks.push(tauri::async_runtime::spawn(async move {
        chat3.cleaner_loop(cleaner_cancel).await;
    }));

    let mut guard = state.bot.lock().map_err(|e| e.to_string())?;
    *guard = Some(BotHandle {
        cancel,
        tasks,
        conn_task,
    });
    *state.chat.lock().map_err(|e| e.to_string())? = Some(chat);
    Ok(())
}

#[tauri::command]
pub async fn stop_bot(state: tauri::State<'_, AppState>) -> Result<(), String> {
    let _restart_guard = state.restart_lock.lock().await;
    let bot = {
        let mut guard = state.bot.lock().map_err(|e| e.to_string())?;
        guard.take()
    };
    if let Some(b) = bot {
        b.stop().await;
        *state.chat.lock().map_err(|e| e.to_string())? = None;
        state.paused.store(false, Ordering::Relaxed);
        state.events.lock().unwrap().push(FrontendEvent::Log {
            level: "info".into(),
            msg: "机器人已停止".into(),
        });
    }
    Ok(())
}

/// 全局暂停/恢复:paused=true 时立即中止所有回复/决策/思考,只保留接收消息
#[tauri::command]
pub async fn set_paused(
    state: tauri::State<'_, AppState>,
    paused: bool,
) -> Result<bool, String> {
    {
        let chat = state.chat.lock().map_err(|e| e.to_string())?.clone();
        if let Some(c) = chat {
            if paused {
                c.stop_all_processing();
            } else {
                c.resume_processing();
            }
        }
        state.paused.store(paused, Ordering::Relaxed);
    }
    state.events.lock().unwrap().push(FrontendEvent::Log {
        level: "info".into(),
        msg: if paused {
            "已停止所有回复/决策/思考,仅接收消息".into()
        } else {
            "已恢复回复".into()
        },
    });
    Ok(paused)
}

// ---------- 测试命令 ----------

#[tauri::command]
pub async fn test_napcat(
    mode: String,
    ws_url: String,
    reverse_port: u16,
    access_token: String,
) -> Result<String, String> {
    if mode == "reverse" {
        let addr = format!("0.0.0.0:{reverse_port}");
        match TcpListener::bind(&addr).await {
            Ok(_) => Ok(format!("✓ 反向 WS 端口 {reverse_port} 可正常监听(请确认 NapCat 已配置 ws-reverse 指向本机:{reverse_port})")),
            Err(e) => Err(format!("✗ 端口 {reverse_port} 无法监听: {e}")),
        }
    } else {
        let mut url = ws_url.trim().to_string();
        if !access_token.trim().is_empty() {
            let sep = if url.contains('?') { '&' } else { '?' };
            url.push(sep);
            url.push_str(&format!("access_token={}", access_token.trim()));
        }
        match tokio::time::timeout(std::time::Duration::from_secs(8), connect_async(&url)).await {
            Ok(Ok((_ws, _))) => Ok("✓ 连接成功,NapCat 在线".into()),
            Ok(Err(e)) => Err(format!("✗ 连接失败: {e}")),
            Err(_) => Err("✗ 连接超时(8s),请确认 NapCat 已启动且地址正确".into()),
        }
    }
}

#[tauri::command]
pub async fn test_llm(m: ModelConfig) -> Result<String, String> {
    if m.api_key.trim().is_empty() {
        return Err("请先填写 API Key".into());
    }
    LlmClient::new().ping(&m).await
}

// ---------- 会话命令 ----------

#[tauri::command]
pub async fn get_sessions(state: tauri::State<'_, AppState>) -> Result<Vec<Value>, String> {
    match running_chat(&state)? {
        Some(c) => Ok(c.session_list().await),
        // 机器人未运行时也返回磁盘会话,列表不再依赖运行状态
        None => Ok(crate::session::scan_session_files(&state.sessions_dir)),
    }
}

#[tauri::command]
pub async fn clear_session(state: tauri::State<'_, AppState>, key: String) -> Result<(), String> {
    match running_chat(&state)? {
        Some(c) => {
            c.clear_session(&key).await;
            Ok(())
        }
        None => Err("机器人未运行".into()),
    }
}

/// 清空会话的历史轨迹(详情页时间线),上下文历史不动
#[tauri::command]
pub async fn clear_trace(state: tauri::State<'_, AppState>, key: String) -> Result<(), String> {
    if let Some(c) = running_chat(&state)? {
        return c.clear_trace(&key);
    }
    // 机器人未运行:直接删文件
    let path = state.sessions_dir.join("traces").join(format!("{key}.jsonl"));
    let _ = std::fs::remove_file(path);
    Ok(())
}

/// 会话详情:轨迹时间线 + 摘要信息(机器人未运行时直接读盘)
#[tauri::command]
pub async fn get_session_detail(
    state: tauri::State<'_, AppState>,
    key: String,
) -> Result<Value, String> {
    if let Some(c) = running_chat(&state)? {
        return Ok(c.session_detail(&key).await);
    }
    // 未运行:磁盘兜底
    let trace_path = state.sessions_dir.join("traces").join(format!("{key}.jsonl"));
    let events = TraceStore::read_all(&trace_path);
    let file = state.sessions_dir.join(format!("{key}.jsonl"));
    let (count, tokens, has_summary, summary) = crate::session::read_history_summary(&file);
    Ok(serde_json::json!({
        "key": key,
        "status": "idle",
        "count": count,
        "tokens": tokens,
        "has_summary": has_summary,
        "summary": summary,
        "events": events,
    }))
}

/// 编辑一条历史消息(改写上下文,同步轨迹;缓存命中率会受影响——由用户自行权衡)
#[tauri::command]
pub async fn update_history_msg(
    state: tauri::State<'_, AppState>,
    key: String,
    id: String,
    text: String,
) -> Result<(), String> {
    if let Some(c) = running_chat(&state)? {
        return c.update_history_msg(&key, &id, &text).await;
    }
    // 机器人未运行:直接改写磁盘(文件为真相,启动后自然生效)
    let ratio = state.config.read().await.chat.estimate_ratio;
    let file = state.sessions_dir.join(format!("{key}.jsonl"));
    crate::session::rewrite_history_entry(&file, &id, &text, ratio)?;
    TraceStore::rewrite_text_by_id(
        &state.sessions_dir.join("traces").join(format!("{key}.jsonl")),
        &id,
        &text,
    );
    Ok(())
}

/// 删除一条历史消息(同时删除轨迹中的对应事件)
#[tauri::command]
pub async fn delete_history_msg(
    state: tauri::State<'_, AppState>,
    key: String,
    id: String,
) -> Result<(), String> {
    if let Some(c) = running_chat(&state)? {
        return c.delete_history_msg(&key, &id).await;
    }
    // 机器人未运行:直接操作磁盘
    let file = state.sessions_dir.join(format!("{key}.jsonl"));
    crate::session::remove_history_entry(&file, &id)?;
    TraceStore::remove_by_id(
        &state.sessions_dir.join("traces").join(format!("{key}.jsonl")),
        &id,
    );
    Ok(())
}

/// 停止进行中的回复(流式请求中止)
#[tauri::command]
pub async fn stop_session(
    state: tauri::State<'_, AppState>,
    key: String,
) -> Result<bool, String> {
    match running_chat(&state)? {
        Some(c) => Ok(c.stop_session(&key)),
        None => Ok(false),
    }
}

// ---------- 记忆命令 ----------

#[tauri::command]
pub async fn get_all_memories(state: tauri::State<'_, AppState>) -> Result<Vec<Value>, String> {
    let dir = state.sessions_dir.join("memories");
    let mut list = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for e in entries.flatten() {
            let fname = e.file_name();
            let fname = fname.to_string_lossy().to_string();
            if let Some(key) = fname.strip_suffix(".jsonl") {
                let mut store = MemoryStore::new(e.path());
                store.refresh();
                list.push(serde_json::json!({
                    "key": key.to_string(),
                    "entries": store.to_values(),
                }));
            }
        }
    }
    list.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
    Ok(list)
}

#[tauri::command]
pub async fn add_memory(
    state: tauri::State<'_, AppState>,
    key: String,
    text: String,
) -> Result<(), String> {
    let cfg = state.config.read().await;
    let mc = &cfg.chat.memory;
    let path = state
        .sessions_dir
        .join("memories")
        .join(format!("{key}.jsonl"));
    let mut store = MemoryStore::new(path);
    store.refresh();
    let added = store.add(&text, "user", mc.max_entries as usize, mc.max_entry_chars as usize);
    if added {
        // 通知运行中的核心:记忆变更(自动控制评估用)
        if let Some(c) = state.chat.lock().unwrap().clone() {
            c.mark_memory_changed();
        }
        Ok(())
    } else {
        Err("记忆为空或已存在".into())
    }
}

#[tauri::command]
pub async fn delete_memory(
    state: tauri::State<'_, AppState>,
    key: String,
    index: usize,
) -> Result<(), String> {
    let path = state
        .sessions_dir
        .join("memories")
        .join(format!("{key}.jsonl"));
    let mut store = MemoryStore::new(path);
    store.refresh();
    if store.remove_index(index) {
        if let Some(c) = state.chat.lock().unwrap().clone() {
            c.mark_memory_changed();
        }
        Ok(())
    } else {
        Err("序号无效".into())
    }
}

// ---------- 开销面板 ----------

/// 查询 DeepSeek 账户余额(GET /user/balance,使用当前激活模型的 key)。
/// 返回 {model, is_available, balances:[{currency, total, granted, topped_up}]}
#[tauri::command]
pub async fn query_balance(state: tauri::State<'_, AppState>) -> Result<Value, String> {
    let model = state
        .config
        .read()
        .await
        .active_model()
        .cloned()
        .ok_or("未配置可用模型".to_string())?;
    let v = LlmClient::new().balance(&model).await?;
    let mut balances = Vec::new();
    for b in v
        .get("balance_infos")
        .and_then(|x| x.as_array())
        .cloned()
        .unwrap_or_default()
    {
        let cur = b.get("currency").and_then(|x| x.as_str()).unwrap_or("");
        let num = |k: &str| -> f64 {
            b.get(k)
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0)
        };
        balances.push(serde_json::json!({
            "currency": cur,
            "total": num("total_balance"),
            "granted": num("granted_balance"),
            "topped_up": num("topped_up_balance"),
        }));
    }
    Ok(serde_json::json!({
        "model": model.name,
        "is_available": v.get("is_available").and_then(|x| x.as_bool()).unwrap_or(false),
        "balances": balances,
    }))
}

/// 开销面板数据:今日聚合 + 分类条形图 + 钱包
#[tauri::command]
pub async fn get_cost_overview(state: tauri::State<'_, AppState>) -> Result<Value, String> {
    let (today, by_category) = {
        // 作用域内取完即释放 std 锁(锁守卫不可跨 await)
        let tracker = state.cost.lock().map_err(|e| e.to_string())?;
        (tracker.today(), tracker.by_category())
    };
    let wallet = state.config.read().await.cost.wallet_balance;
    let remaining = wallet - today.cost;
    Ok(serde_json::json!({
        "today": today,
        "by_category": by_category,
        "wallet_balance": wallet,
        "remaining": remaining,
    }))
}

// ---------- 记忆位置审批 ----------

/// 当前待审批的切换提案(前端兜底轮询;常规走 PlacementProposal 事件)
#[tauri::command]
pub async fn get_placement_proposal(
    state: tauri::State<'_, AppState>,
) -> Result<Option<Value>, String> {
    let ctl = state.placement.lock().map_err(|e| e.to_string())?;
    Ok(ctl.pending.clone().map(|p| serde_json::json!(p)))
}

/// 审批记忆位置切换提案:approve=true 时落盘切换并保存配置
#[tauri::command]
pub async fn approve_placement(
    state: tauri::State<'_, AppState>,
    approve: bool,
) -> Result<Option<String>, String> {
    // 提案仍与当前方案不同才应用(用户可能已手动改过);锁守卫不可跨 await,先取当前值
    let (current, cooldown_secs) = {
        let cfg = state.config.read().await;
        (
            cfg.chat.memory.placement.clone(),
            (cfg.chat.memory.auto_cooldown_minutes.max(1) * 60) as i64,
        )
    };
    let applied = {
        let mut ctl = state.placement.lock().map_err(|e| e.to_string())?;
        let now = crate::trace::now_ts();
        let applied = if approve {
            ctl.pending
                .as_ref()
                .filter(|p| p.to != current)
                .map(|p| p.to.clone())
        } else {
            None
        };
        ctl.settle(now, cooldown_secs);
        applied
    };
    if let Some(to) = applied {
        {
            let mut cfg = state.config.write().await;
            cfg.chat.memory.placement = to.clone();
        }
        {
            let cfg = state.config.read().await;
            config::save_config(&state.cfg_path, &cfg)?;
        }
        let _ = state.events.lock().unwrap().push(FrontendEvent::Log {
            level: "info".into(),
            msg: format!("记忆位置已切换 -> {to}(自动控制,用户批准)"),
        });
        // 机器人运行中:重启使新位置立即生效
        let _restart_guard = state.restart_lock.lock().await;
        let old_bot = {
            let mut guard = state.bot.lock().map_err(|e| e.to_string())?;
            guard.take()
        };
        if let Some(b) = old_bot {
            b.stop().await;
            start_bot_inner(&state).await?;
        }
        Ok(Some(to))
    } else {
        Ok(None)
    }
}

// ---------- 状态展示 ----------

#[derive(Serialize, Clone)]
pub struct StatusView {
    pub running: bool,
    pub connected: bool,
    pub mode: String,
    pub endpoint: String,
    pub self_id: Option<String>,
    pub last_error: String,
    /// 全局暂停回复(仅接收消息)
    pub paused: bool,
}

#[tauri::command]
pub async fn get_status_view(state: tauri::State<'_, AppState>) -> Result<StatusView, String> {
    let running = state.bot.lock().map_err(|e| e.to_string())?.is_some();
    let paused = state.paused.load(Ordering::Relaxed);
    let (mode, endpoint, self_id) = {
        let cfg = state.config.read().await;
        let n = &cfg.napcat;
        let endpoint = if n.mode == "reverse" {
            format!("0.0.0.0:{}", n.reverse_port)
        } else {
            n.ws_url.clone()
        };
        let sid = n.self_id.trim().to_string();
        (n.mode.clone(), endpoint, if sid.is_empty() { None } else { Some(sid) })
    };
    // 最近连接状态快照(事件链路外的前端兜底)
    let (connected, live_self, last_error) = {
        let g = state.last_status.lock().map_err(|e| e.to_string())?;
        match g.as_ref() {
            Some(s) => (s.connected, s.self_id, s.last_error.clone()),
            None => (false, None, String::new()),
        }
    };
    Ok(StatusView {
        running,
        connected,
        mode,
        endpoint,
        self_id: self_id.or(live_self.map(|i| i.to_string())),
        last_error,
        paused,
    })
}
