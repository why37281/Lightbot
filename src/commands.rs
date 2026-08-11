//! Tauri 命令层:前端(GUI)调用入口 + 机器人生命周期管理。

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::Value;
use tauri::{Emitter, Manager};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, RwLock};
use tokio_tungstenite::connect_async;
use tokio_util::sync::CancellationToken;

use crate::chat::{ChatCore, FrontendEvent};
use crate::config::{self, Config, ModelConfig};
use crate::llm::LlmClient;
use crate::memory::MemoryStore;
use crate::napcat::{BotEvent, ConnStatus, NapcatClient};

// ---------- 应用状态 ----------

pub struct AppState {
    pub cfg_path: PathBuf,
    pub sessions_dir: PathBuf,
    pub config: Arc<RwLock<Config>>,
    /// 前端事件总线
    pub ev_tx: mpsc::Sender<FrontendEvent>,
    pub bot: Mutex<Option<BotHandle>>,
    /// 运行中的对话核心(命令层会话操作复用)
    pub chat: Mutex<Option<Arc<ChatCore>>>,
    /// 启动/重启互斥(防并发 start_bot 与 save_config 重启竞态)
    pub restart_lock: tokio::sync::Mutex<()>,
    /// 最近一次连接状态快照(get_status_view 兜底用,不依赖事件链路)
    pub last_status: Arc<Mutex<Option<ConnStatus>>>,
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

    let (ev_tx, mut ev_rx) = mpsc::channel::<FrontendEvent>(1024);
    let handle = app.handle().clone();
    tauri::async_runtime::spawn(async move {
        while let Some(ev) = ev_rx.recv().await {
            let _ = handle.emit("frontend", ev);
        }
    });

    app.manage(AppState {
        cfg_path,
        sessions_dir,
        config: Arc::new(RwLock::new(loaded)),
        ev_tx,
        bot: Mutex::new(None),
        chat: Mutex::new(None),
        restart_lock: tokio::sync::Mutex::new(()),
        last_status: Arc::new(Mutex::new(None)),
    });
    Ok(())
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
    // 校验:至少一个模型且名称唯一
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
    if cfg.models.iter().any(|m| !(0.0..=2.0).contains(&m.temperature)) {
        return Err("温度需在 0~2 之间".into());
    }

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
        let _ = state
            .ev_tx
            .try_send(FrontendEvent::Log {
                level: "info".into(),
                msg: "配置已保存,机器人已按新配置重启".into(),
            });
    }
    Ok(())
}

#[tauri::command]
pub fn get_config_path(state: tauri::State<'_, AppState>) -> String {
    state.cfg_path.display().to_string()
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

    let chat = Arc::new(ChatCore::new(
        state.config.clone(),
        sender,
        state.sessions_dir.clone(),
        state.cfg_path.clone(),
        state.ev_tx.clone(),
    ));

    let mut tasks = Vec::new();

    // 事件管线:消息 -> 对话;通知/请求 -> 前端展示
    let chat2 = chat.clone();
    let ev_tx2 = state.ev_tx.clone();
    tasks.push(tauri::async_runtime::spawn(async move {
        while let Some(ev) = bot_ev_rx.recv().await {
            match ev {
                BotEvent::Message(m) => chat2.handle_message(m).await,
                BotEvent::Notice(n) => {
                    let _ = ev_tx2.try_send(FrontendEvent::Notice {
                        desc: n.desc,
                        notice_type: n.notice_type,
                    });
                }
                BotEvent::Request(r) => {
                    let _ = ev_tx2.try_send(FrontendEvent::Notice {
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
                    let _ = ev_tx2.try_send(FrontendEvent::Log {
                        level: "info".into(),
                        msg: format!("连接事件: {l}"),
                    });
                }
            }
        }
    }));

    // 连接状态 -> 前端(同时写快照供 get_status_view 兜底)
    let ev_tx3 = state.ev_tx.clone();
    let last_status = state.last_status.clone();
    tasks.push(tauri::async_runtime::spawn(async move {
        while status_rx.changed().await.is_ok() {
            let s = status_rx.borrow().clone();
            *last_status.lock().unwrap() = Some(s.clone());
            let _ = ev_tx3.try_send(FrontendEvent::Status { status: s.clone() });
            let _ = ev_tx3.try_send(FrontendEvent::Log {
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
        let _ = state.ev_tx.try_send(FrontendEvent::Log {
            level: "info".into(),
            msg: "机器人已停止".into(),
        });
    }
    Ok(())
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
    let chat = state.chat.lock().map_err(|e| e.to_string())?.clone();
    match chat {
        Some(c) => Ok(c.session_list().await),
        // 机器人未运行时也返回磁盘会话,列表不再依赖运行状态
        None => Ok(crate::chat::scan_session_files(&state.sessions_dir)),
    }
}

#[tauri::command]
pub async fn clear_session(state: tauri::State<'_, AppState>, key: String) -> Result<(), String> {
    let chat = state.chat.lock().map(|c| c.clone()).unwrap_or(None);
    match chat {
        Some(c) => {
            c.clear_session(&key).await;
            Ok(())
        }
        None => Err("机器人未运行".into()),
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
        Ok(())
    } else {
        Err("序号无效".into())
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
}

#[tauri::command]
pub async fn get_status_view(state: tauri::State<'_, AppState>) -> Result<StatusView, String> {
    let running = state.bot.lock().map_err(|e| e.to_string())?.is_some();
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
    })
}
