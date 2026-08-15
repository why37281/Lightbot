//! 会话与上下文管理 —— 缓存优化核心。
//!
//! ## 缓存友好原则(针对 DeepSeek 自动 context caching)
//!
//! 1. 稳定前缀:system(人设)固定在最前,摘要紧随其后;
//! 2. 头部截断:超出预算时只从最旧消息开始丢弃,剩余部分保持逐字节不变 → 前缀哈希命中;
//! 3. 记忆位置分两方案(详见 `placement.rs`):
//!    - 方案一(back):[人设][摘要][历史][记忆][轨迹][提问] —— 记忆随历史增长每轮 miss;
//!    - 方案二(front):[人设][摘要][记忆][新历史][轨迹][提问] —— 记忆稳定命中,
//!      新历史超过 `recent_max_tokens` 时把超出「保留条数」的部分折叠进摘要;
//! 4. 会话持久化(JSONL):重启后同一会话继续追加,缓存可跨重启延续。
//!
//! 每个会话串行处理(try_lock 防堆积),token 估算轻量(字符级,可配保守系数)。
//!
//! ## 会话状态机
//!
//! 空闲 → (决策中,仅决策器开启时) → 回复中 → 思考中 → 回复中 → 空闲。
//! 「思考中」只覆盖首 token 之后仍在输出 reasoning 的阶段(首 token 前的网络等待算回复中);
//! 执行中 / 审批中 为 agent 功能预留的占位状态。

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{watch, Mutex, RwLock};

use crate::config::{Config, MemoryConfig, ModelConfig, TrailConfig};
use crate::cost::{CostTracker, UsageRecord};
use crate::llm::{ApiMessage, LlmClient, StreamEvent, Usage, USER_STOPPED};
use crate::memory::{self, MemoryOp, MemoryStore};
use crate::napcat::{ActionSender, ConnStatus, MsgKind, ParsedMsg};
use crate::placement::{self, Eval, PlacementController, Prices, Scheme};
use crate::trace::{self, TraceEvent, TraceStore};
use crate::trigger;

// ---------- 前端事件 ----------

/// 记忆管理说明:恒定的独立 system 消息(与开关无关,开关切换不影响缓存前缀)。
/// ⚠️ 此文本内容改动会破坏缓存前缀,勿随意修改。
pub const MEMORY_GUIDE: &str = "(你可以管理长期记忆:当你了解到值得长期记住的信息(用户偏好、重要事实、约定)时,在回复末尾用标记 [记忆:添加 内容] 写入;需要删除时用 [记忆:删除 内容片段]。不要写入临时性信息,每次只写最重要的。)";

/// 思考长度约束:恒定的独立 system 消息(与思考开关无关,缓存前缀稳定)。
/// ⚠️ 此文本内容改动会破坏缓存前缀,勿随意修改。
pub const THINKING_GUIDE: &str = "(思考过程请保持简短:只需要确定接下来该回复什么即可,不要展开长篇推理。)";

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FrontendEvent {
    Log { level: String, msg: String },
    Status { status: ConnStatus },
    MsgIn { key: String, text: String },
    MsgOut { key: String, text: String },
    LlmStats {
        model: String,
        prompt_tokens: u64,
        completion_tokens: u64,
        cache_hit: u64,
        cache_miss: u64,
        reasoning_tokens: u64,
        elapsed_ms: u64,
    },
    SessionChanged {
        key: String,
        count: usize,
        tokens: u64,
    },
    Notice { desc: String, notice_type: String },
    /// 会话状态变化(会话列表胶囊灯)
    SessionStatus { key: String, status: String },
    /// 完整轨迹事件(会话详情页时间线 + 落盘)
    Trace { key: String, entry: TraceEvent },
    /// 流式增量(暂未启用:详情页流式显示走 live 缓冲轮询)
    TurnDelta {
        key: String,
        turn: String,
        kind: String,
        text: String,
    },
    /// 记忆位置切换提案(醒目弹窗审批)
    PlacementProposal { proposal: placement::Proposal },
}

/// 事件环形缓冲(拉模式):前端通过 get_events 轮询拉取,不再依赖 Tauri 推送事件
/// (推送链路在部分环境下不可用,invoke 拉取已被证明可靠)。
pub struct EventBuf {
    next_seq: u64,
    events: VecDeque<(u64, FrontendEvent)>,
}

impl EventBuf {
    pub fn new() -> Self {
        Self {
            next_seq: 1,
            events: VecDeque::new(),
        }
    }

    pub fn push(&mut self, ev: FrontendEvent) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.events.push_back((seq, ev));
        // 环形上限:防止长时间运行无限增长(日志面板自身也只保留 2000 行)
        while self.events.len() > 2000 {
            self.events.pop_front();
        }
    }

    /// 返回 seq > after 的全部事件与当前最新 seq(事件被环形淘汰时前端靠 latest 对齐)
    pub fn after(&self, after: u64) -> (Vec<(u64, FrontendEvent)>, u64) {
        let mut out = Vec::new();
        for (seq, ev) in &self.events {
            if *seq > after {
                out.push((*seq, ev.clone()));
            }
        }
        (out, self.next_seq.saturating_sub(1))
    }
}

impl Default for EventBuf {
    fn default() -> Self {
        Self::new()
    }
}

// ---------- token 估算 ----------

/// 轻量字符级 token 估算:ASCII 约 4 字符/token,其余(中文等)约 1.3 字符/token,再乘保守系数
pub fn estimate_tokens(s: &str, ratio: f64) -> u32 {
    let mut ascii = 0usize;
    let mut other = 0usize;
    for c in s.chars() {
        if c.is_ascii() {
            ascii += 1;
        } else {
            other += 1;
        }
    }
    ((ascii as f64 / 4.0 + other as f64 * 1.3) * ratio) as u32 + 1
}

/// 计算需要从头部丢弃的条数(缓存友好:只删头部;至少保留 min_keep 条)
pub fn compute_drop(hist_tokens: &[u32], budget: u64, min_keep: usize) -> usize {
    let mut total: u64 = hist_tokens.iter().map(|t| *t as u64).sum();
    let mut drop = 0usize;
    while total > budget && hist_tokens.len() - drop > min_keep {
        total = total.saturating_sub(hist_tokens[drop] as u64);
        drop += 1;
    }
    drop
}

// ---------- 会话 ----------

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct HistoryMsg {
    pub role: String,
    pub text: String,
    #[serde(default)]
    pub ts: i64,
    /// 估算 token(落盘,便于文件级统计)
    #[serde(default)]
    pub tokens: u32,
    /// 稳定 id(与轨迹联动,详情页编辑/删除用;旧文件缺省时加载时生成)
    #[serde(default)]
    pub id: String,
}

pub struct Session {
    pub key: String,
    pub history: Vec<HistoryMsg>,
    pub summary: Option<String>,
    pub summary_tokens: u32,
    pub last_active: Instant,
    pub file: PathBuf,
    pub loaded: bool,
    /// 长期记忆(独立文件 memories/{key}.jsonl,文件为唯一真相)
    pub memory: MemoryStore,
}

impl Session {
    fn new(key: &str, dir: &std::path::Path) -> Self {
        Self {
            key: key.to_string(),
            history: Vec::new(),
            summary: None,
            summary_tokens: 0,
            last_active: Instant::now(),
            file: dir.join(format!("{key}.jsonl")),
            loaded: false,
            memory: MemoryStore::new(dir.join("memories").join(format!("{key}.jsonl"))),
        }
    }

    /// 懒加载历史(JSONL;首行若为 __summary__ 则为折叠摘要)
    fn ensure_loaded(&mut self) {
        if self.loaded {
            return;
        }
        self.loaded = true;
        if let Ok(content) = std::fs::read_to_string(&self.file) {
            for line in content.lines() {
                let mut h = match serde_json::from_str::<HistoryMsg>(line) {
                    Ok(h) => h,
                    Err(_) => continue,
                };
                if h.role == "__summary__" {
                    self.summary_tokens = estimate_tokens(&h.text, 1.15);
                    self.summary = Some(h.text);
                    continue;
                }
                if h.id.is_empty() {
                    h.id = trace::new_id();
                }
                self.history.push(h);
            }
        }
    }

    /// 编辑后强制从磁盘重载(GUI 改写了历史文件)
    pub fn reload_from_disk(&mut self) {
        self.loaded = false;
        self.history.clear();
        self.summary = None;
        self.summary_tokens = 0;
        self.ensure_loaded();
    }

    /// 全量重写会话文件(摘要 + 历史),用于截断/摘要折叠后保证磁盘与内存一致
    fn rewrite(&self) {
        if let Some(dir) = self.file.parent() {
            let _ = std::fs::create_dir_all(dir);
            let mut out = String::new();
            if let Some(s) = &self.summary {
                let line = HistoryMsg {
                    role: "__summary__".into(),
                    text: s.clone(),
                    ts: 0,
                    tokens: 0,
                    id: String::new(),
                };
                if let Ok(l) = serde_json::to_string(&line) {
                    out.push_str(&l);
                    out.push('\n');
                }
            }
            for h in &self.history {
                if let Ok(l) = serde_json::to_string(h) {
                    out.push_str(&l);
                    out.push('\n');
                }
            }
            let _ = std::fs::write(&self.file, out);
        }
    }

    /// 追加一条并落盘,返回该条 token 估算
    fn push_id(&mut self, role: &str, text: &str, ratio: f64, id: &str) -> u32 {
        let tokens = estimate_tokens(text, ratio);
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let h = HistoryMsg {
            role: role.to_string(),
            text: text.to_string(),
            ts,
            tokens,
            id: id.to_string(),
        };
        if let Some(dir) = self.file.parent() {
            let _ = std::fs::create_dir_all(dir);
            let line = serde_json::to_string(&h).unwrap_or_default();
            use std::io::Write;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.file)
            {
                let _ = writeln!(f, "{line}");
            }
        }
        self.history.push(h);
        tokens
    }

    /// 追加一条(自动生成 id;测试与外部工具使用)
    #[cfg(test)]
    fn push(&mut self, role: &str, text: &str, ratio: f64) {
        let id = trace::new_id();
        self.push_id(role, text, ratio, &id);
    }

    fn clear(&mut self) {
        self.history.clear();
        self.summary = None;
        self.summary_tokens = 0;
        let _ = std::fs::remove_file(&self.file);
    }

    fn total_tokens(&self) -> u64 {
        self.history.iter().map(|h| h.tokens as u64).sum::<u64>()
            + self.summary_tokens as u64
    }
}

/// 从磁盘读取会话摘要信息(供详情页与列表;机器人未运行也可用)
pub fn read_history_summary(file: &Path) -> (usize, u64, bool, Option<String>) {
    let mut count = 0usize;
    let mut tokens = 0u64;
    let mut has_summary = false;
    let mut summary = None;
    if let Ok(content) = std::fs::read_to_string(file) {
        for line in content.lines() {
            if let Ok(h) = serde_json::from_str::<HistoryMsg>(line) {
                if h.role == "__summary__" {
                    has_summary = true;
                    summary = Some(h.text);
                    continue;
                }
                count += 1;
                tokens += h.tokens as u64;
            }
        }
    }
    (count, tokens, has_summary, summary)
}

// ---------- 会话状态 ----------

/// 进行中回复的直播缓冲(事件链路之外的前端兜底:轮询会话详情也能看到流式进展)
#[derive(Serialize, Clone, Debug, Default)]
pub struct LiveTurn {
    pub turn: String,
    /// 已累计的思考内容
    pub reasoning: String,
    /// 已累计的正文内容
    pub content: String,
}

/// 会话状态(执行中/审批中为 agent 功能预留的占位状态,当前未启用)
#[allow(dead_code)]
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Idle,
    Replying,
    Thinking,
    Deciding,
    Executing,
    Approval,
}

impl SessionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionStatus::Idle => "idle",
            SessionStatus::Replying => "replying",
            SessionStatus::Thinking => "thinking",
            SessionStatus::Deciding => "deciding",
            SessionStatus::Executing => "executing",
            SessionStatus::Approval => "approval",
        }
    }
    pub fn label(&self) -> &'static str {
        match self {
            SessionStatus::Idle => "空闲",
            SessionStatus::Replying => "回复中",
            SessionStatus::Thinking => "思考中",
            SessionStatus::Deciding => "决策中",
            SessionStatus::Executing => "执行中",
            SessionStatus::Approval => "审批中",
        }
    }
}

// ---------- 业务核心 ----------

pub struct ChatCore {
    pub cfg: Arc<RwLock<Config>>,
    pub llm: LlmClient,
    pub sender: ActionSender,
    pub sessions: RwLock<HashMap<String, Arc<Mutex<Session>>>>,
    pub sessions_dir: PathBuf,
    /// 配置文件路径(/model、/prompt 命令切换后自动保存)
    pub cfg_path: PathBuf,
    /// 前端事件缓冲(拉模式:前端经 get_events 轮询取走)
    pub events: Arc<StdMutex<EventBuf>>,
    /// 群活跃度跟踪:key -> 最近窗口内消息时间戳(插话采样用)
    pub activity: StdMutex<HashMap<String, VecDeque<Instant>>>,
    /// 每群消息计数(插话条数冷却)
    pub msg_count: StdMutex<HashMap<String, u64>>,
    /// 每群最近一次主动发言时的消息计数(插话冷却,软 at 也会刷新)
    pub interject_at: StdMutex<HashMap<String, u64>>,
    /// 群聊轨迹:未触发对话的普通消息缓冲(key -> (时间, 文本))
    pub trail: StdMutex<HashMap<String, VecDeque<(SystemTime, String)>>>,
    /// 会话状态(列表胶囊灯)
    pub status: StdMutex<HashMap<String, SessionStatus>>,
    /// 进行中回复的中止通道(key -> sender)
    pub aborts: StdMutex<HashMap<String, watch::Sender<bool>>>,
    /// 会话忙碌时暂存的消息队列(key -> (消息, turn)),回合结束后按序补处理
    pub pending_msgs: StdMutex<HashMap<String, VecDeque<(ParsedMsg, String)>>>,
    /// 每会话消息串行门(跨会话并行:同群保序,不同群互不阻塞)
    pub msg_gates: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    /// 全局暂停回复(与命令层共享):true 时只接收消息,不决策/不回复/不思考
    pub paused: Arc<AtomicBool>,
    /// 决策器全局中止通道(暂停时取消进行中的决策请求)
    pub decide_cancel: watch::Sender<bool>,
    /// 进行中回复的直播缓冲(key -> 已累计思考/正文;轮询兜底用)
    pub live: StdMutex<HashMap<String, LiveTurn>>,
    /// 费用追踪(与命令层共享)
    pub cost: Arc<StdMutex<CostTracker>>,
    /// 记忆位置自动控制状态(与命令层共享)
    pub placement: Arc<StdMutex<PlacementController>>,
    /// 记忆变更计数(评估变更概率用)
    pub mem_changes: AtomicU64,
    /// 轨迹目录
    pub trace_dir: PathBuf,
}

impl ChatCore {
    pub fn new(
        cfg: Arc<RwLock<Config>>,
        sender: ActionSender,
        sessions_dir: PathBuf,
        cfg_path: PathBuf,
        events: Arc<StdMutex<EventBuf>>,
        cost: Arc<StdMutex<CostTracker>>,
        placement: Arc<StdMutex<PlacementController>>,
        paused: Arc<AtomicBool>,
        decide_cancel: watch::Sender<bool>,
    ) -> Self {
        let trace_dir = sessions_dir.join("traces");
        Self {
            cfg,
            llm: LlmClient::new(),
            sender,
            sessions: RwLock::new(HashMap::new()),
            trace_dir,
            sessions_dir,
            cfg_path,
            events,
            activity: StdMutex::new(HashMap::new()),
            msg_count: StdMutex::new(HashMap::new()),
            interject_at: StdMutex::new(HashMap::new()),
            trail: StdMutex::new(HashMap::new()),
            status: StdMutex::new(HashMap::new()),
            aborts: StdMutex::new(HashMap::new()),
            pending_msgs: StdMutex::new(HashMap::new()),
            msg_gates: RwLock::new(HashMap::new()),
            live: StdMutex::new(HashMap::new()),
            cost,
            placement,
            paused,
            decide_cancel,
            mem_changes: AtomicU64::new(0),
        }
    }

    fn log(&self, level: &str, msg: &str) {
        self.events.lock().unwrap().push(FrontendEvent::Log {
            level: level.to_string(),
            msg: msg.to_string(),
        });
    }

    /// 会话状态变化:状态表更新 + 事件入环形缓冲(前端经 get_events 拉取)
    async fn set_status(&self, key: &str, s: SessionStatus) {
        {
            let mut m = self.status.lock().unwrap();
            let cur = m.entry(key.to_string()).or_insert(SessionStatus::Idle);
            if *cur == s {
                return;
            }
            *cur = s;
        }
        self.events.lock().unwrap().push(FrontendEvent::SessionStatus {
            key: key.to_string(),
            status: s.as_str().to_string(),
        });
    }

    pub fn get_status(&self, key: &str) -> SessionStatus {
        self.status
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or(SessionStatus::Idle)
    }

    /// 每会话消息串行门(跨会话并行:同群保序,不同群互不阻塞)
    async fn get_gate(&self, key: &str) -> Arc<Mutex<()>> {
        {
            let map = self.msg_gates.read().await;
            if let Some(g) = map.get(key) {
                return g.clone();
            }
        }
        let mut map = self.msg_gates.write().await;
        map.entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// 全局暂停:中止所有进行中的流式回复与决策请求、清空排队。
    /// 暂停后消息仍接收/记录,但不再决策与回复;resume_processing 恢复。
    pub fn stop_all_processing(&self) {
        self.paused.store(true, Ordering::Relaxed);
        for tx in self.aborts.lock().unwrap().values() {
            let _ = tx.send(true);
        }
        self.pending_msgs.lock().unwrap().clear();
        let _ = self.decide_cancel.send(true);
    }

    pub fn resume_processing(&self) {
        let _ = self.decide_cancel.send(false);
        self.paused.store(false, Ordering::Relaxed);
    }

    // ---------- 轨迹与费用 ----------

    async fn trace_push(&self, key: &str, ev: &TraceEvent) {
        self.events.lock().unwrap().push(FrontendEvent::Trace {
            key: key.to_string(),
            entry: ev.clone(),
        });
        let store = TraceStore::new(self.trace_dir.join(format!("{key}.jsonl")));
        store.push(ev);
    }

    fn turn_delta(&self, key: &str, turn: &str, kind: &str, text: &str) {
        // 详情页流式显示已改走 live 缓冲轮询,增量事件不再入缓冲(避免挤占日志事件)
        let _ = (key, turn, kind, text);
    }

    fn record_usage(&self, model: &ModelConfig, category: &str, usage: &Usage) {
        self.cost.lock().unwrap().record(UsageRecord {
            ts: trace::now_ts(),
            model: model.model.clone(),
            category: category.to_string(),
            prompt: usage.prompt_tokens,
            cache_hit: usage.cache_hit,
            cache_miss: usage.cache_miss,
            completion: usage.completion_tokens,
            reasoning: usage.reasoning_tokens,
            price_input: model.price_input,
            price_cache_hit: model.price_cache_hit,
            price_output: model.price_output,
        });
    }

    async fn emit_llm_stats(&self, model: &ModelConfig, usage: &Usage, elapsed_ms: u64) {
        self.events.lock().unwrap().push(FrontendEvent::LlmStats {
            model: model.model.clone(),
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            cache_hit: usage.cache_hit,
            cache_miss: usage.cache_miss,
            reasoning_tokens: usage.reasoning_tokens,
            elapsed_ms,
        });
    }

    /// 记忆发生变更时递增计数(模型写入/删除、/remember、/forget、GUI 增删)
    pub fn mark_memory_changed(&self) {
        self.mem_changes.fetch_add(1, Ordering::Relaxed);
    }

    // ---------- 事件入口 ----------

    /// 事件入口:消息分流 —— 被动触发/软 at/命令走完整通道,插话采样走轻量通道。
    /// 每会话串行门:同群消息严格保序,不同群/私聊并行处理(互不阻塞)。
    pub async fn handle_message(&self, msg: ParsedMsg) {
        if msg.is_self {
            return;
        }
        let key = session_key(&msg);
        // 每会话串行门(跨会话并行):同群消息严格保序,不同群/私聊互不阻塞
        let gate = self.get_gate(&key).await;
        let _gate = gate.lock().await;
        let turn = trace::new_id();
        self.emit_msg_in(&key, &msg.text);

        // 全局暂停:只接收消息(记录日志与时间线),不决策、不回复、不思考
        if self.paused.load(Ordering::Relaxed) {
            self.log("info", &format!("[{key}] 已暂停回复,仅接收: {}", msg.text));
            self.trace_push(
                &key,
                &TraceEvent::MsgIn {
                    id: None,
                    turn,
                    ts: trace::now_ts(),
                    trigger: "paused".into(),
                    text: msg.text.clone(),
                    ignored: true,
                },
            )
            .await;
            return;
        }

        let cfg = self.cfg.read().await.clone();
        // 活跃度窗口(可配置,默认 2 分钟)
        let win_min = cfg.chat.interject.activity_window_minutes.max(1);
        self.track_activity(&key, win_min * 60);

        // * 前缀消息:完全忽略(不回复、不入历史、不触发),给群友自由交流空间
        if cfg.chat.ignore_star && msg.text.trim_start().starts_with('*') {
            self.log("info", &format!("[{key}] * 前缀消息,忽略: {}", msg.text));
            self.trace_push(
                &key,
                &TraceEvent::MsgIn {
                    id: None,
                    turn,
                    ts: trace::now_ts(),
                    trigger: "ignored".into(),
                    text: msg.text.clone(),
                    ignored: true,
                },
            )
            .await;
            return;
        }

        // 斜杠命令:群聊/私聊均可直接触发,跳过决策器(命令绝不因决策器被吞)
        let is_cmd = msg.text.trim_start().starts_with('/');
        if is_cmd {
            self.full_dialogue(&key, &msg, &turn).await;
            return;
        }

        // ① 被动触发(@/回复/关键词/私聊)→ 完整通道
        if trigger::passive_hit(&cfg, &msg) {
            drop(cfg);
            let hint = if msg.at_me {
                "对方 @ 了你"
            } else if msg.reply_me {
                "对方引用回复了你的消息"
            } else if msg.kind == MsgKind::Private {
                "这是发给你的私聊消息"
            } else {
                "消息包含触发关键词"
            };
            if self.decider_ok(&key, &turn, &msg.text, hint).await {
                self.full_dialogue(&key, &msg, &turn).await;
            } else {
                self.log("info", &format!("[{key}] 决策器:无需回复(被动触发)"));
                self.trace_push(
                    &key,
                    &TraceEvent::MsgIn {
                        id: None,
                        turn: turn.clone(),
                        ts: trace::now_ts(),
                        trigger: "decided_no".into(),
                        text: msg.text.clone(),
                        ignored: true,
                    },
                )
                .await;
            }
            return;
        }
        // ② 软 at(提到机器人称呼)→ 完整通道,必回,刷新插话冷却(决策器通过才回复)
        if msg.kind == MsgKind::Group && trigger::soft_at_hit(&cfg, &msg.text) {
            drop(cfg);
            if self.decider_ok(&key, &turn, &msg.text, "消息提到了你的称呼").await {
                self.mark_interjected(&key);
                self.log("info", &format!("[{key}] 软 at 触发(称呼提及)"));
                self.full_dialogue(&key, &msg, &turn).await;
            } else {
                self.log("info", &format!("[{key}] 决策器:无需回复(软 at)"));
                self.trace_push(
                    &key,
                    &TraceEvent::MsgIn {
                        id: None,
                        turn: turn.clone(),
                        ts: trace::now_ts(),
                        trigger: "decided_no".into(),
                        text: msg.text.clone(),
                        ignored: true,
                    },
                )
                .await;
            }
            return;
        }
        // ③ 插话采样 → 轻量通道(群聊,概率/固定频率 + 冷却;决策器通过才插话)
        let user_text = trigger::strip_keyword(&msg.text, &cfg.napcat.keyword).to_string();
        if msg.kind == MsgKind::Group
            && !user_text.is_empty()
            && self.interject_sample(&key, &user_text).await
        {
            drop(cfg);
            if self.decider_ok(&key, &turn, &user_text, "主动插话采样命中(是否接话)").await {
                self.log("info", &format!("[{key}] 主动插话: {user_text}"));
                // 完整上下文开关:开启后插话走完整通道(历史/记忆/轨迹按设置注入,并记入历史);
                // 关闭则走轻量通道(单轮、不落盘)。完整通道的消息不进轨迹,避免重复注入。
                let full_context = {
                    let c = self.cfg.read().await;
                    c.chat.interject.full_context
                };
                if full_context {
                    self.full_dialogue(&key, &msg, &turn).await;
                } else {
                    self.record_trail(&key, &msg.text).await;
                    self.light_reply(&msg, &user_text, &turn).await;
                }
            } else {
                // 决策拒绝也消耗本次插话机会,防止高频重试
                self.mark_interjected(&key);
                self.log("info", &format!("[{key}] 决策器:无需回复(插话)"));
                self.trace_push(
                    &key,
                    &TraceEvent::MsgIn {
                        id: None,
                        turn: turn.clone(),
                        ts: trace::now_ts(),
                        trigger: "decided_no".into(),
                        text: msg.text.clone(),
                        ignored: true,
                    },
                )
                .await;
            }
        } else {
            // 全部触发条件未命中:记录群聊轨迹(解决"鱼的记忆"),并写入时间线
            // (修复:此前未触发消息不进轨迹文件,详情页看不到,显得"被吞掉")
            self.log("debug", &format!("[{key}] 未触发回复逻辑: {}", msg.text));
            self.record_trail(&key, &msg.text).await;
            self.trace_push(
                &key,
                &TraceEvent::MsgIn {
                    id: None,
                    turn,
                    ts: trace::now_ts(),
                    trigger: "untriggered".into(),
                    text: msg.text.clone(),
                    ignored: true,
                },
            )
            .await;
        }
    }

    fn trigger_label(&self, msg: &ParsedMsg) -> String {
        if msg.kind == MsgKind::Private {
            "private".into()
        } else if msg.at_me {
            "at".into()
        } else if msg.reply_me {
            "reply".into()
        } else {
            "keyword".into()
        }
    }

    /// 记录群聊轨迹(仅群聊;窗口与条数按注入模式取配置)。
    /// window 模式按 window_minutes 保留;all / triggered_only 保留 24 小时。
    /// all 模式不设条数上限(用户显式选择「全部注入,无上限」);
    /// triggered_only 仍以 max_entries 作为缓冲安全上限。
    async fn record_trail(&self, key: &str, text: &str) {
        if !key.starts_with('g') {
            return;
        }
        let (win, max, mode) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.trail.window_minutes.max(1) * 60,
                cfg.chat.trail.max_entries as usize,
                cfg.chat.trail.inject_mode.clone(),
            )
        };
        let window_secs = if mode == "window" { win } else { 86400 };
        // all 模式:max_entries 传 0 表示不设条数上限
        let max_entries = if mode == "all" { 0 } else { max };
        self.trail_push(key, text, window_secs, max_entries);
    }

    /// 决策器:开启时由当前模型判断这条消息是否需要回复。
    /// 关闭 / 无模型 / 决策调用失败 → 按需要回复处理(保守,不漏回消息)。
    /// trigger_hint 说明消息的触发方式(修复:决策器此前看不到 @ 信息,把召唤消息误判为闲聊)
    async fn decider_ok(&self, key: &str, turn: &str, text: &str, trigger_hint: &str) -> bool {
        let (enabled, model, prompt) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.decider,
                cfg.active_model().cloned(),
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
            )
        };
        if !enabled {
            return true;
        }
        let Some(model) = model else {
            return true;
        };
        self.set_status(key, SessionStatus::Deciding).await;
        let started = Instant::now();
        // 全局暂停时中止决策请求(select 丢弃 future 即取消 HTTP 请求)
        let mut cancel_rx = self.decide_cancel.subscribe();
        let result = tokio::select! {
            r = self.llm.decide(&model, &prompt, text, trigger_hint) => Some(r),
            _ = cancel_rx.changed() => None,
        };
        self.set_status(key, SessionStatus::Idle).await;
        let ms = started.elapsed().as_millis() as u64;
        match result {
            None => {
                // 暂停中止:不回复(与"仅接收"语义一致)
                self.log("info", &format!("[{key}] 已暂停,决策中止"));
                self.trace_push(
                    key,
                    &TraceEvent::Error {
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: "已暂停,决策中止".into(),
                    },
                )
                .await;
                false
            }
            Some(Ok((yes, usage))) => {
                self.record_usage(&model, "decide", &usage);
                self.emit_llm_stats(&model, &usage, ms).await;
                self.trace_push(
                    key,
                    &TraceEvent::Decide {
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: text.to_string(),
                        verdict: yes,
                        model: model.model.clone(),
                        ms,
                    },
                )
                .await;
                self.log(
                    "info",
                    &format!(
                        "决策器[{trigger_hint}]: {}这条消息 ({}ms)",
                        if yes { "需要回复" } else { "无需回复" },
                        ms
                    ),
                );
                yes
            }
            Some(Err(e)) => {
                self.log("warn", &format!("决策器调用失败,按需要回复处理: {e}"));
                self.trace_push(
                    key,
                    &TraceEvent::Error {
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: format!("决策器失败: {e}"),
                    },
                )
                .await;
                true
            }
        }
    }

    /// 完整通道:会话级对话(命令 / 上下文预算 / 历史 / 落盘)
    async fn full_dialogue(&self, key: &str, msg: &ParsedMsg, turn: &str) {
        // 会话内串行:busy 时排队而非丢弃(热闹群里 @ 消息不丢失,回合结束按序补处理)
        let sess = self.get_session(key).await;
        let Ok(mut session) = sess.try_lock() else {
            self.queue_pending(key, msg.clone(), turn.to_string());
            self.log("info", &format!("[{key}] 上一条消息仍在处理,本条已排队"));
            return;
        };
        session.ensure_loaded();
        session.last_active = Instant::now();
        self.log(
            "debug",
            &format!("[{key}] 会话加载: 历史 {} 条", session.history.len()),
        );

        // 命令处理(先于一切模型调用;返回 Some(回复) 表示已处理)
        let text = msg.text.trim();
        if text.starts_with('/') {
            if let Some(reply) = self.handle_command(&mut session, msg, text).await {
                self.trace_push(
                    key,
                    &TraceEvent::Cmd {
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: text.to_string(),
                        reply,
                    },
                )
                .await;
                drop(session);
                self.drain_queue(key).await;
                return;
            }
        }

        // 群聊触发时,剥离关键词前缀
        let user_text = {
            let cfg = self.cfg.read().await;
            trigger::strip_keyword(text, &cfg.napcat.keyword).to_string()
        };
        if user_text.is_empty() {
            drop(session);
            self.drain_queue(key).await;
            return;
        }

        // 上下文预算管理(按当前记忆方案折叠/截断)
        self.trim_context(&mut session, &user_text, turn).await;

        // 快照本轮所需配置(避免持锁跨 await)
        let (prompt, model, mem_cfg, trail_cfg, ratio, reserve, pending_cfg) = {
            let cfg = self.cfg.read().await;
            (
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.active_model().cloned(),
                cfg.chat.memory.clone(),
                cfg.chat.trail.clone(),
                cfg.chat.estimate_ratio,
                cfg.chat.reserve_tokens,
                (
                    cfg.napcat.reply_pending,
                    cfg.napcat.pending_text.clone(),
                    cfg.napcat.pending_delay_secs,
                ),
            )
        };
        let Some(model) = model else {
            self.log("error", "未配置可用模型");
            drop(session);
            self.drain_queue(key).await;
            return;
        };

        // 组装消息流(方案一/二由 mem_cfg.placement 决定;缓存友好顺序)
        let msgs = self
            .build_messages(
                &session,
                key,
                msg,
                &prompt,
                &user_text,
                &mem_cfg,
                &trail_cfg,
                ratio,
            )
            .await;

        // 先落用户消息(即使模型失败也在历史里),再记轨迹
        let user_id = trace::new_id();
        let user_tokens = session.push_id("user", &user_text, ratio, &user_id) as f64;
        self.emit_session(key, &session).await;
        self.trace_push(
            key,
            &TraceEvent::MsgIn {
                id: Some(user_id),
                turn: turn.to_string(),
                ts: trace::now_ts(),
                trigger: self.trigger_label(msg),
                text: user_text.clone(),
                ignored: false,
            },
        )
        .await;

        // 回复中 → 流式调用(思考中/回复中由增量自动切换;首 token 前的等待算回复中)
        self.set_status(key, SessionStatus::Replying).await;
        let started = Instant::now();
        let (abort_tx, abort_rx) = watch::channel(false);
        self.aborts.lock().unwrap().insert(key.to_string(), abort_tx);

        let (stream_result, pending_sent) = self
            .run_streamed_chat(key, turn, msg, &model, &msgs, reserve, abort_rx, pending_cfg, started)
            .await;

        self.aborts.lock().unwrap().remove(key);
        self.set_status(key, SessionStatus::Idle).await;

        match stream_result {
            Ok(reply) => {
                self.record_usage(&model, "dialogue", &reply.usage);
                let elapsed = started.elapsed().as_millis() as u64;
                self.emit_llm_stats(&model, &reply.usage, elapsed).await;
                self.log(
                    "info",
                    &format!(
                        "[{key}] {}({}t, 缓存命中率 {:.0}%, {}ms{})",
                        model.model,
                        reply.usage.prompt_tokens,
                        reply.usage.hit_ratio() * 100.0,
                        elapsed,
                        if reply.usage.reasoning_tokens > 0 {
                            format!(", 思考 {}t", reply.usage.reasoning_tokens)
                        } else {
                            String::new()
                        }
                    ),
                );
                // 思考过程落轨迹(直播增量已上抛,这里存全量)
                if !reply.reasoning.is_empty() {
                    self.trace_push(
                        key,
                        &TraceEvent::Think {
                            turn: turn.to_string(),
                            ts: trace::now_ts(),
                            text: reply.reasoning.clone(),
                            tokens: reply.usage.reasoning_tokens,
                        },
                    )
                    .await;
                }
                // 记忆标记:总是剥离(防止关闭状态泄漏到群里);仅开启时执行
                let mut out = reply.text;
                let (clean, ops) = memory::parse_memory_ops(&out);
                out = clean;
                let mut mem_changed = false;
                if mem_cfg.enabled {
                    for op in ops {
                        match op {
                            MemoryOp::Add(text) => {
                                if session.memory.add(
                                    &text,
                                    "model",
                                    mem_cfg.max_entries as usize,
                                    mem_cfg.max_entry_chars as usize,
                                ) {
                                    mem_changed = true;
                                    self.log("info", &format!("[{key}] 模型写入记忆: {text}"));
                                }
                            }
                            MemoryOp::Remove(needle) => {
                                if session.memory.remove_contains(&needle) > 0 {
                                    mem_changed = true;
                                    self.log("info", &format!("[{key}] 模型删除记忆: {needle}"));
                                }
                            }
                        }
                    }
                }
                if mem_changed {
                    self.mark_memory_changed();
                }
                // 思考过程(reasoning_content)只用于统计,绝不发送给用户
                if out.is_empty() {
                    out = "(模型未返回内容)".into();
                }
                let send_result = self.send_text(msg, &out).await;
                // 落盘助手消息 + 轨迹(发送失败也记录真实输出)
                let assistant_id = trace::new_id();
                let assistant_tokens = session.push_id("assistant", &out, ratio, &assistant_id) as f64;
                self.emit_session(key, &session).await;
                self.trace_push(
                    key,
                    &TraceEvent::MsgOut {
                        id: Some(assistant_id),
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: out.clone(),
                        model: model.model.clone(),
                        usage: reply.usage.clone(),
                    },
                )
                .await;
                if let Err(e) = send_result {
                    self.log("warn", &format!("[{key}] 回复发送失败: {e}"));
                }
                // 记忆位置自动评估(每轮喂指标,窗口满时评估)
                self.feed_placement_round(user_tokens + assistant_tokens);
                self.evaluate_placement(&session).await;
            }
            Err(e) => {
                let stopped = e == USER_STOPPED;
                self.trace_push(
                    key,
                    &TraceEvent::Error {
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: if stopped {
                            "已停止".to_string()
                        } else {
                            e.clone()
                        },
                    },
                )
                .await;
                if stopped {
                    // 已发过"思考中"提示则补一条停止告知,否则静默(用户自己按的停止)
                    if pending_sent {
                        let _ = self.send_text(msg, "⏹ 回复已停止。").await;
                    }
                } else {
                    self.log("error", &format!("[{key}] 模型调用失败: {e}"));
                    let _ = self.send_text(msg, &format!("⚠️ 出错了: {e}")).await;
                }
            }
        }
        // 释放会话锁后补处理排队消息(排队消息的决策器在到达时已运行)
        drop(session);
        self.drain_queue(key).await;
    }

    /// 忙碌时入队(上限 8 条,超出丢最旧,防无限堆积)
    fn queue_pending(&self, key: &str, msg: ParsedMsg, turn: String) {
        let mut m = self.pending_msgs.lock().unwrap();
        let q = m.entry(key.to_string()).or_default();
        if q.len() >= 8 {
            q.pop_front();
        }
        q.push_back((msg, turn));
    }

    /// 逐条补处理排队消息(full_dialogue 结束处调用;嵌套调用自然处理新增排队)
    async fn drain_queue(&self, key: &str) {
        loop {
            let next = {
                let mut m = self.pending_msgs.lock().unwrap();
                m.get_mut(key).and_then(|q| q.pop_front())
            };
            let Some((msg, turn)) = next else { return; };
            let sess = self.get_session(key).await;
            if sess.try_lock().is_err() {
                // 理论不应发生(锁已释放);放回队首,留待下次
                self.pending_msgs
                    .lock()
                    .unwrap()
                    .entry(key.to_string())
                    .or_default()
                    .push_front((msg, turn));
                return;
            }
            // Box::pin 打断 async 递归的类型膨胀(full_dialogue → drain_queue → full_dialogue)
            Box::pin(self.full_dialogue(key, &msg, &turn)).await;
        }
    }

    /// 流式调用驱动:增量上抛 + 状态切换 + 思考提示计时。
    /// 返回(结果, 是否已发思考提示)。
    #[allow(clippy::too_many_arguments)]
    async fn run_streamed_chat(
        &self,
        key: &str,
        turn: &str,
        msg: &ParsedMsg,
        model: &ModelConfig,
        msgs: &[ApiMessage],
        reserve: u32,
        cancel: watch::Receiver<bool>,
        pending_cfg: (bool, String, u64),
        started: Instant,
    ) -> (Result<crate::llm::LlmReply, String>, bool) {
        let mut stream = match self
            .llm
            .chat_stream(model, msgs, Some(reserve), cancel)
            .await
        {
            Ok(s) => s,
            Err(e) => return (Err(e), false),
        };
        let (pending_enabled, pending_text, pending_delay) = pending_cfg;
        let mut pending_sent = false;
        let mut first_token_at: Option<Instant> = None;
        let mut got_content = false;
        loop {
            tokio::select! {
                ev = stream.next_event() => {
                    match ev {
                        None => break,
                        Some(Err(e)) => {
                            self.live.lock().unwrap().remove(key);
                            return (Err(e), pending_sent);
                        }
                        Some(Ok(StreamEvent::Reasoning { delta })) => {
                            if first_token_at.is_none() {
                                first_token_at = Some(Instant::now());
                            }
                            self.set_status(key, SessionStatus::Thinking).await;
                            self.turn_delta(key, turn, "think", &delta);
                            // 直播缓冲(前端轮询兜底)
                            {
                                let mut m = self.live.lock().unwrap();
                                let lt = m.entry(key.to_string()).or_insert_with(|| LiveTurn {
                                    turn: turn.to_string(),
                                    reasoning: String::new(),
                                    content: String::new(),
                                });
                                lt.reasoning.push_str(&delta);
                            }
                        }
                        Some(Ok(StreamEvent::Content { delta })) => {
                            if first_token_at.is_none() {
                                first_token_at = Some(Instant::now());
                            }
                            got_content = true;
                            self.set_status(key, SessionStatus::Replying).await;
                            self.turn_delta(key, turn, "out", &delta);
                            {
                                let mut m = self.live.lock().unwrap();
                                let lt = m.entry(key.to_string()).or_insert_with(|| LiveTurn {
                                    turn: turn.to_string(),
                                    reasoning: String::new(),
                                    content: String::new(),
                                });
                                lt.content.push_str(&delta);
                            }
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => {
                    // 思考提示:首 token 之后的纯思考超过阈值才发(首 token 前的等待不计入);
                    // 长时间无首 token 视为网络停滞,兜底提示
                    let stall = first_token_at.is_none()
                        && started.elapsed()
                            >= Duration::from_secs(pending_delay.saturating_mul(2).saturating_add(15));
                    let thinking_too_long = first_token_at
                        .map(|t| !got_content && t.elapsed() >= Duration::from_secs(pending_delay))
                        .unwrap_or(false);
                    if pending_enabled && !pending_sent && (thinking_too_long || stall) {
                        let _ = self.send_text(msg, &pending_text).await;
                        pending_sent = true;
                    }
                }
            }
        }
        self.live.lock().unwrap().remove(key);
        (Ok(stream.finish()), pending_sent)
    }

    /// 组装消息流(缓存友好顺序,方案由 placement 决定):
    /// 方案二(front):[人设][记忆说明][摘要][记忆][历史][轨迹][提问]
    /// 方案一(back): [人设][记忆说明][摘要][历史][记忆][轨迹][提问]
    async fn build_messages(
        &self,
        session: &Session,
        key: &str,
        msg: &ParsedMsg,
        prompt: &str,
        user_text: &str,
        mem_cfg: &MemoryConfig,
        trail_cfg: &TrailConfig,
        ratio: f64,
    ) -> Vec<ApiMessage> {
        let mut msgs = vec![
            ApiMessage {
                role: "system".into(),
                content: prompt.to_string(),
            },
            ApiMessage {
                role: "system".into(),
                content: MEMORY_GUIDE.to_string(),
            },
            ApiMessage {
                role: "system".into(),
                content: THINKING_GUIDE.to_string(),
            },
        ];
        if let Some(s) = &session.summary {
            msgs.push(ApiMessage {
                role: "system".into(),
                content: format!("[先前对话摘要]\n{s}"),
            });
        }
        // 记忆内容(trim_context 中已刷新与裁剪)
        let mut mem_msg: Option<ApiMessage> = None;
        if mem_cfg.enabled && !session.memory.entries.is_empty() {
            mem_msg = Some(ApiMessage {
                role: "system".into(),
                content: session.memory.system_text(),
            });
        }
        if mem_cfg.placement != "back" {
            if let Some(m) = mem_msg.clone() {
                msgs.push(m);
            }
        }
        for h in &session.history {
            msgs.push(ApiMessage {
                role: h.role.clone(),
                content: h.text.clone(),
            });
        }
        if mem_cfg.placement == "back" {
            if let Some(m) = mem_msg {
                msgs.push(m);
            }
        }
        // 群聊轨迹:最近未触发消息(位于最后,变化不影响历史/记忆缓存)。
        // 注入行为三档:
        //  window          所有完整对话都注入,按时间窗口过滤(默认);
        //  all             所有完整对话都注入,不受时间窗口限制(缓冲保留 24h);
        //  triggered_only  仅 @ / 引用触发的对话注入。
        if trail_cfg.enabled && msg.kind == MsgKind::Group {
            let inject = match trail_cfg.inject_mode.as_str() {
                "triggered_only" => msg.at_me || msg.reply_me,
                _ => true,
            };
            if inject {
                let lines = self
                    .trail
                    .lock()
                    .unwrap()
                    .get(key)
                    .cloned()
                    .unwrap_or_default();
                let window_secs = if trail_cfg.inject_mode == "window" {
                    trail_cfg.window_minutes.max(1) * 60
                } else {
                    86400
                };
                // all 模式:max_tokens 传 0 = 不设 token 上限,全部注入
                let max_tokens = if trail_cfg.inject_mode == "all" {
                    0
                } else {
                    trail_cfg.max_tokens
                };
                if let Some(content) = render_trail(&lines, window_secs, max_tokens, ratio) {
                    msgs.push(ApiMessage {
                        role: "user".into(),
                        content,
                    });
                }
            }
        }
        msgs.push(ApiMessage {
            role: "user".into(),
            content: user_text.to_string(),
        });
        msgs
    }

    /// 轻量通道:单轮插话(极小上下文、不落盘、不新开会话、失败静默)
    async fn light_reply(&self, msg: &ParsedMsg, user_text: &str, turn: &str) {
        let (prompt, model, max_tokens) = {
            let cfg = self.cfg.read().await;
            (
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.active_model().cloned(),
                cfg.chat.interject.interject_max_tokens.max(16),
            )
        };
        let Some(model) = model else {
            return;
        };
        let key = session_key(msg);
        let msgs = vec![
            ApiMessage {
                role: "system".into(),
                content: format!(
                    "{prompt}\n\n(现在是群聊中的随口插话场景:请用一两句话自然、口语化地回应,不要称呼任何人,不要使用列表或长篇大论。\n思考过程请保持简短:只需要确定该接什么话即可。)"
                ),
            },
            ApiMessage {
                role: "user".into(),
                content: user_text.to_string(),
            },
        ];
        let started = Instant::now();
        match self.llm.chat(&model, &msgs, Some(max_tokens)).await {
            Ok(reply) => {
                self.record_usage(&model, "interject", &reply.usage);
                self.emit_llm_stats(&model, &reply.usage, started.elapsed().as_millis() as u64).await;
                // 插话场景剥离记忆标记但不执行(轻量通道不管理记忆)
                let (text, _) = memory::parse_memory_ops(&reply.text);
                let out = if text.is_empty() {
                    "(模型未返回内容)".to_string()
                } else {
                    text
                };
                let _ = self.send_text(msg, &out).await;
                self.trace_push(
                    &key,
                    &TraceEvent::LiteOut {
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: out,
                        model: model.model.clone(),
                    },
                )
                .await;
            }
            Err(e) => {
                // 插话失败静默,不打扰群
                self.log("warn", &format!("[{key}] 插话失败: {e}"));
                self.trace_push(
                    &key,
                    &TraceEvent::Error {
                        turn: turn.to_string(),
                        ts: trace::now_ts(),
                        text: format!("插话失败: {e}"),
                    },
                )
                .await;
            }
        }
    }

    // ---------- 记忆位置自动控制 ----------

    fn feed_placement_round(&self, growth_tokens: f64) {
        let mut ctl = self.placement.lock().unwrap();
        let cur = self.mem_changes.load(Ordering::Relaxed);
        let changed = cur != ctl.mem_changes;
        ctl.mem_changes = cur;
        ctl.feed_round(changed, growth_tokens);
    }

    async fn evaluate_placement(&self, session: &Session) {
        let (auto, placement_str, ratio, recent_cap, keep_msgs, sum_cap) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.memory.auto_placement,
                cfg.chat.memory.placement.clone(),
                cfg.chat.estimate_ratio,
                cfg.chat.recent_max_tokens as f64,
                cfg.chat.recent_keep_msgs as f64,
                cfg.chat.summarize_tokens as f64,
            )
        };
        if !auto {
            return;
        }
        let prices = {
            let cfg = self.cfg.read().await;
            match cfg.active_model() {
                Some(m) => Prices {
                    input: m.price_input,
                    cache_hit: m.price_cache_hit,
                    output: m.price_output,
                },
                None => Prices {
                    input: 1.0,
                    cache_hit: 0.02,
                    output: 2.0,
                },
            }
        };
        let mut ctl = self.placement.lock().unwrap();
        if ctl.rounds_in_window < placement::EVAL_EVERY {
            return;
        }
        let hist = session.total_tokens() as f64;
        let eval = Eval {
            mem_tokens: session.memory.total_tokens(ratio) as f64,
            recent_tokens: hist.min(recent_cap),
            summary_tokens: session.summary_tokens as f64,
            history_tokens: hist,
            p_change: ctl.p_change,
            growth: ctl.growth,
            recent_cap: recent_cap.max(1.0),
            keep_msgs,
            summary_cap: sum_cap.max(64.0),
            prices,
        };
        if let Some(p) = ctl.evaluate(&eval, Scheme::from_placement(&placement_str), trace::now_ts())
        {
            self.log("info", &format!("记忆位置评估:建议切换到 {}", p.to));
            self.events
                .lock()
                .unwrap()
                .push(FrontendEvent::PlacementProposal { proposal: p });
        }
    }

    // ---------- 命令 ----------

    /// 内置斜杠命令,返回 Some(回复文本) 表示已处理。
    /// ⚠️ 锁纪律:先快照所需配置再执行;写锁只在无读锁时获取
    /// (历史 bug:/model、/prompt 持读锁跨写锁 await,造成死锁)。
    async fn handle_command(&self, session: &mut Session, msg: &ParsedMsg, text: &str) -> Option<String> {
        let (model_names, prompt_ids, mem_cfg) = {
            let cfg = self.cfg.read().await;
            (
                cfg.models.iter().map(|m| m.name.clone()).collect::<Vec<_>>(),
                cfg.prompts.iter().map(|p| p.id.clone()).collect::<Vec<_>>(),
                cfg.chat.memory.clone(),
            )
        };
        let reply: String = match text {
            "/clear" => {
                session.clear();
                // 关键修复:清空上下文必须同时清空群聊轨迹,否则未触发消息
                // (如发过的令牌)仍会通过轨迹注入,导致「/clear 之后机器人还记得」
                self.trail.lock().unwrap().remove(&session.key);
                self.emit_session(&session.key, session).await;
                self.log("info", &format!("[{}] 已清空上下文与群聊轨迹", session.key));
                "🧹 上下文与群聊轨迹已清空,重新开始对话。".to_string()
            }
            "/stats" => {
                let toks = session.total_tokens();
                self.log(
                    "info",
                    &format!(
                        "[{}] 查看统计: {} 条消息,约 {} tokens",
                        session.key,
                        session.history.len(),
                        toks
                    ),
                );
                format!(
                    "📊 当前会话:{} 条消息,约 {} tokens{}",
                    session.history.len(),
                    toks,
                    session
                        .summary
                        .as_ref()
                        .map(|_| "(含摘要)")
                        .unwrap_or("")
                )
            }
            _ => {
                if let Some(rest) = text.strip_prefix("/remember ") {
                    let content = rest.trim();
                    if content.is_empty() {
                        "用法:/remember <内容>".to_string()
                    } else {
                        session.memory.refresh();
                        let ok = session.memory.add(
                            content,
                            "user",
                            mem_cfg.max_entries as usize,
                            mem_cfg.max_entry_chars as usize,
                        );
                        if ok {
                            self.mark_memory_changed();
                        }
                        self.log(
                            "info",
                            &format!(
                                "[{}] 用户添加记忆{}: {content}",
                                session.key,
                                if ok { "" } else { "(重复或为空)" }
                            ),
                        );
                        if ok {
                            format!("🧠 已记住: {content}")
                        } else {
                            "这条记忆为空或已存在。".to_string()
                        }
                    }
                } else if let Some(rest) = text.strip_prefix("/forget ") {
                    let content = rest.trim();
                    session.memory.refresh();
                    // 仅支持英文逗号分隔的数字序号,如 /forget 1,3
                    let mut idxs: Vec<usize> = Vec::new();
                    let mut valid = true;
                    for part in content.split(',') {
                        let p = part.trim();
                        if p.is_empty() {
                            continue;
                        }
                        match p.parse::<usize>() {
                            Ok(i) => idxs.push(i),
                            Err(_) => {
                                valid = false;
                                break;
                            }
                        }
                    }
                    if !valid || idxs.is_empty() {
                        "用法:/forget <序号,逗号分隔,如 1,3>".to_string()
                    } else {
                        idxs.sort_unstable();
                        idxs.dedup();
                        // 从大到小删除,避免序号漂移
                        let mut removed = 0;
                        for idx in idxs.into_iter().rev() {
                            if session.memory.remove_index(idx) {
                                removed += 1;
                            }
                        }
                        if removed > 0 {
                            self.mark_memory_changed();
                        }
                        self.log(
                            "info",
                            &format!(
                                "[{}] 用户删除记忆: 序号 [{}],删除 {removed} 条",
                                session.key, content
                            ),
                        );
                        if removed > 0 {
                            format!("🗑️ 已删除 {removed} 条记忆。")
                        } else {
                            "未找到匹配的序号。".to_string()
                        }
                    }
                } else if text == "/memories" {
                    session.memory.refresh();
                    if session.memory.entries.is_empty() {
                        "🧠 暂无记忆。".to_string()
                    } else {
                        let scope = if msg.kind == MsgKind::Group {
                            "本群"
                        } else {
                            "本会话"
                        };
                        let mut s = format!(
                            "🧠 {scope}长期记忆({} 条):\n",
                            session.memory.entries.len()
                        );
                        for (i, e) in session.memory.entries.iter().enumerate() {
                            s.push_str(&format!(
                                "{}. [{} {}] {}\n",
                                i + 1,
                                if e.source == "model" { "自动" } else { "用户" },
                                crate::memory::fmt_ts(e.ts),
                                e.text
                            ));
                        }
                        s.push_str("\n💡 添加:/remember <内容> · 删除:/forget <序号,如 1,3>");
                        self.log(
                            "info",
                            &format!("[{}] 查看记忆: {} 条", session.key, session.memory.entries.len()),
                        );
                        s
                    }
                } else if text == "/model" {
                    format!("可用模型: {}", model_names.join(", "))
                } else if let Some(rest) = text.strip_prefix("/model ") {
                    let name = rest.trim();
                    if model_names.iter().any(|m| m == name) {
                        // 快照已 drop,此时可安全获取写锁(修复死锁)
                        {
                            let mut cfg = self.cfg.write().await;
                            cfg.active_model = name.to_string();
                        }
                        {
                            let cfg = self.cfg.read().await;
                            let _ = crate::config::save_config(&self.cfg_path, &cfg);
                        }
                        self.log("info", &format!("[{}] 切换模型 -> {name}", session.key));
                        format!("✅ 已切换模型: {name}")
                    } else {
                        format!("❌ 未找到模型 {name},可用: {}", model_names.join(", "))
                    }
                } else if text == "/prompt" {
                    format!("可用人设: {}", prompt_ids.join(", "))
                } else if let Some(rest) = text.strip_prefix("/prompt ") {
                    let id = rest.trim();
                    if prompt_ids.iter().any(|p| p == id) {
                        {
                            let mut cfg = self.cfg.write().await;
                            cfg.active_prompt = id.to_string();
                        }
                        {
                            let cfg = self.cfg.read().await;
                            let _ = crate::config::save_config(&self.cfg_path, &cfg);
                        }
                        self.log("info", &format!("[{}] 切换人设 -> {id}", session.key));
                        format!("✅ 已切换人设: {id}")
                    } else {
                        format!("❌ 未找到人设 {id},可用: {}", prompt_ids.join(", "))
                    }
                } else {
                    return None;
                }
            }
        };
        let _ = self.send_text(msg, &reply).await;
        Some(reply)
    }

    // ---------- 上下文预算 ----------

    /// 上下文预算管理:按记忆方案折叠 + 兜底头部截断(缓存友好)。
    async fn trim_context(&self, session: &mut Session, user_text: &str, turn: &str) {
        let (budget, summarize, summarize_tokens, ratio, prompt, mem_cfg, history_target, recent_cap, recent_keep) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.context_tokens.saturating_sub(cfg.chat.reserve_tokens) as u64,
                cfg.chat.summarize,
                cfg.chat.summarize_tokens,
                cfg.chat.estimate_ratio,
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.chat.memory.clone(),
                cfg.chat.history_target_tokens,
                cfg.chat.recent_max_tokens as u64,
                cfg.chat.recent_keep_msgs as usize,
            )
        };
        let (prompt_tok, user_tok) = {
            // 固定前缀 token:人设 + 记忆说明 + 思考约束(恒定) + 当前提问
            let pt = (estimate_tokens(&prompt, ratio)
                + estimate_tokens(MEMORY_GUIDE, ratio)
                + estimate_tokens(THINKING_GUIDE, ratio)) as u64;
            (pt, estimate_tokens(user_text, ratio) as u64)
        };
        // 记忆:刷新 + 超预算裁剪最旧条目(保护上下文预算)
        let mut mem_tok: u64 = 0;
        if mem_cfg.enabled {
            session.memory.refresh();
            session.memory.trim_to_tokens(mem_cfg.max_tokens.max(64), ratio);
            mem_tok = session.memory.total_tokens(ratio) as u64;
        }
        let mut sum_tok = session.summary_tokens as u64;
        let hist_tok: u64 = session.history.iter().map(|h| h.tokens as u64).sum();

        // 预算报告(debug):裁剪前的占用明细
        self.log(
            "debug",
            &format!(
                "[{}] 上下文预算: 人设{prompt_tok} + 记忆{mem_tok} + 摘要{sum_tok} + 历史{hist_tok}({}条) / 总预算 {budget}t",
                session.key,
                session.history.len()
            ),
        );

        // 摘要与历史共享的预算(输入预算扣除人设、记忆与当前提问)
        let input_budget = budget.saturating_sub(prompt_tok + mem_tok + user_tok);
        if input_budget <= sum_tok && session.summary.is_some() {
            session.summary = None;
            sum_tok = 0;
            session.rewrite();
        }

        // 折叠策略:按记忆方案分派(均可关闭)
        if summarize && session.history.len() > 2 {
            let drop = if mem_cfg.placement == "front" {
                // 方案二:新历史超阈值时折叠到 ≤ recent_cap(至少保留最近 keep 条);
                // 折叠后历史从 keep 条重新积累,自然形成折叠周期(与成本模型一致)
                let tokens: Vec<u32> = session.history.iter().map(|h| h.tokens).collect();
                compute_drop(&tokens, recent_cap, recent_keep.max(1))
            } else if history_target > 0 {
                // 方案一:历史超过主动折叠阈值(或逼近预算)时折叠,至少丢 2 条
                let fold_budget = input_budget
                    .saturating_sub(sum_tok)
                    .min(history_target as u64);
                let tokens: Vec<u32> = session.history.iter().map(|h| h.tokens).collect();
                if hist_tok > fold_budget {
                    compute_drop(&tokens, fold_budget, 1).max(2)
                } else {
                    0
                }
            } else {
                0
            };
            if drop > 0 {
                self.fold_history(session, drop, summarize_tokens, ratio, turn)
                    .await;
            }
        }

        // 兜底:无论方案与开关,超出预算时必须头部截断(修复历史 bug:关闭摘要时永不截断)
        let final_budget = input_budget.saturating_sub(session.summary_tokens as u64);
        let tokens: Vec<u32> = session.history.iter().map(|h| h.tokens).collect();
        let drop = compute_drop(&tokens, final_budget, 0);
        if drop > 0 {
            session.history.drain(..drop);
            session.rewrite();
            self.log(
                "info",
                &format!("[{}] 上下文超预算,丢弃最旧 {} 条消息", session.key, drop),
            );
        }
    }

    /// 把最旧 drop 条折叠进摘要(失败则丢弃并落盘,保证磁盘与内存一致)
    async fn fold_history(
        &self,
        session: &mut Session,
        drop: usize,
        summarize_tokens: u32,
        ratio: f64,
        turn: &str,
    ) {
        let dropped: Vec<HistoryMsg> = session.history.drain(..drop).collect();
        let old_summary = session.summary.clone().unwrap_or_default();
        let api_msgs: Vec<ApiMessage> = dropped
            .iter()
            .map(|h| ApiMessage {
                role: h.role.clone(),
                content: h.text.clone(),
            })
            .collect();
        let model = self.cfg.read().await.active_model().cloned();
        match model {
            Some(model) => match self
                .llm
                .summarize(&model, &old_summary, &api_msgs, summarize_tokens)
                .await
            {
                Ok((s, usage)) => {
                    self.record_usage(&model, "summarize", &usage);
                    session.summary = Some(s.clone());
                    session.summary_tokens = estimate_tokens(&s, ratio);
                    session.rewrite();
                    self.trace_push(
                        &session.key,
                        &TraceEvent::Fold {
                            turn: turn.to_string(),
                            ts: trace::now_ts(),
                            folded: dropped.len(),
                            summary_tokens: session.summary_tokens,
                        },
                    )
                    .await;
                    self.log(
                        "info",
                        &format!(
                            "[{}] 已折叠 {} 条旧消息为摘要",
                            session.key,
                            dropped.len()
                        ),
                    );
                }
                Err(e) => {
                    self.log("warn", &format!("[{}] 摘要生成失败: {e}", session.key));
                    session.summary = None;
                    session.summary_tokens = 0;
                    session.rewrite();
                }
            },
            None => {
                // 无可用模型:丢弃部分也需落盘,保证磁盘与内存一致
                session.rewrite();
                self.log(
                    "info",
                    &format!(
                        "[{}] 无可用模型,直接丢弃最旧 {} 条消息",
                        session.key,
                        dropped.len()
                    ),
                );
            }
        }
    }

    // ---------- 会话管理 ----------

    async fn get_session(&self, key: &str) -> Arc<Mutex<Session>> {
        {
            let map = self.sessions.read().await;
            if let Some(s) = map.get(key) {
                return s.clone();
            }
        }
        let mut map = self.sessions.write().await;
        map.entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(Session::new(key, &self.sessions_dir))))
            .clone()
    }

    async fn reload_session(&self, key: &str) -> Result<(), String> {
        let map = self.sessions.read().await;
        let Some(sess) = map.get(key) else {
            return Ok(());
        };
        let mut s = sess
            .try_lock()
            .map_err(|_| "会话正在回复中,请稍后再编辑".to_string())?;
        s.reload_from_disk();
        self.emit_session(key, &s).await;
        Ok(())
    }

    /// 发送回复(自动分段)
    async fn send_text(&self, msg: &ParsedMsg, text: &str) -> Result<(), String> {
        let (max_len, delay) = {
            let cfg = self.cfg.read().await;
            (cfg.napcat.max_msg_len.max(1), cfg.napcat.segment_delay_ms)
        };
        let chunks: Vec<String> = text
            .chars()
            .collect::<Vec<_>>()
            .chunks(max_len)
            .map(|c| c.iter().collect())
            .collect();
        if chunks.len() > 1 {
            self.log(
                "info",
                &format!("[{}] 回复较长,分 {} 段发送", session_key(msg), chunks.len()),
            );
        }
        for (i, chunk) in chunks.iter().enumerate() {
            let r = match msg.kind {
                MsgKind::Group => {
                    self.sender
                        .send_group_msg(msg.group_id.unwrap_or(0), chunk)
                        .await
                }
                MsgKind::Private => self.sender.send_private_msg(msg.user_id, chunk).await,
            };
            if let Err(e) = r {
                self.log(
                    "warn",
                    &format!(
                        "[{}] 发送失败(第 {}/{} 段,共 {} 字): {e}",
                        session_key(msg),
                        i + 1,
                        chunks.len(),
                        text.chars().count()
                    ),
                );
                return Err(e);
            }
            self.emit_msg_out(&session_key(msg), chunk);
            if i + 1 < chunks.len() {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
        Ok(())
    }

    fn emit_msg_in(&self, key: &str, text: &str) {
        self.events.lock().unwrap().push(FrontendEvent::MsgIn {
            key: key.to_string(),
            text: text.to_string(),
        });
    }

    fn emit_msg_out(&self, key: &str, text: &str) {
        self.events.lock().unwrap().push(FrontendEvent::MsgOut {
            key: key.to_string(),
            text: text.to_string(),
        });
    }

    async fn emit_session(&self, key: &str, session: &Session) {
        self.events.lock().unwrap().push(FrontendEvent::SessionChanged {
            key: key.to_string(),
            count: session.history.len(),
            tokens: session.total_tokens(),
        });
    }

    // ---------- 活跃度与插话 ----------

    /// 记录群活跃度(滑动窗口,跨度可配置),同时递增消息计数(插话冷却用)
    fn track_activity(&self, key: &str, window_secs: u64) {
        let mut m = self.activity.lock().unwrap();
        let q = m.entry(key.to_string()).or_default();
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(window_secs);
        while q.front().map(|t| *t < cutoff).unwrap_or(false) {
            q.pop_front();
        }
        q.push_back(now);
        drop(m);
        *self
            .msg_count
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert(0) += 1;
    }

    /// 消息速率(条/分钟):窗口内消息数 / 窗口分钟数
    fn activity_rate(&self, key: &str, window_minutes: u64) -> f64 {
        let m = self.activity.lock().unwrap();
        m.get(key)
            .map(|q| q.len() as f64 / window_minutes.max(1) as f64)
            .unwrap_or(0.0)
    }

    fn mark_interjected(&self, key: &str) {
        let count = self.msg_count.lock().unwrap().get(key).copied().unwrap_or(0);
        self.interject_at
            .lock()
            .unwrap()
            .insert(key.to_string(), count);
    }

    /// 记录群聊轨迹(未触发对话的消息),按窗口与条数限制。
    /// max_entries == 0 表示不设条数上限(「全部注入」模式)。
    fn trail_push(&self, key: &str, text: &str, window_secs: u64, max_entries: usize) {
        let text = text.trim();
        if text.is_empty() {
            return;
        }
        let mut m = self.trail.lock().unwrap();
        let q = m.entry(key.to_string()).or_default();
        let now = SystemTime::now();
        q.retain(|(t, _)| {
            now.duration_since(*t)
                .map(|d| d.as_secs() < window_secs)
                .unwrap_or(false)
        });
        q.push_back((now, text.to_string()));
        if max_entries > 0 {
            while q.len() > max_entries {
                q.pop_front();
            }
        }
    }

    /// 插话采样:开关 + 冷却 + 概率(基线/钩子词/水消息 + 活跃度缩放)或固定频率
    async fn interject_sample(&self, key: &str, text: &str) -> bool {
        let cfg = self.cfg.read().await;
        let ij = &cfg.chat.interject;
        if !ij.enabled || ij.mode == "off" {
            return false;
        }
        // 冷却:距上次主动发言以来,群里新消息达到阈值才允许插话。
        // fixed_rate 模式阈值 = rate_every_messages(默认每 5 条);其余模式 = cooldown_messages。
        let count = self.msg_count.lock().unwrap().get(key).copied().unwrap_or(0);
        let need = if ij.mode == "fixed_rate" {
            ij.rate_every_messages.max(1) as u64
        } else {
            ij.cooldown_messages.max(1) as u64
        };
        if let Some(last) = self.interject_at.lock().unwrap().get(key) {
            let since = count.saturating_sub(*last);
            if since < need {
                self.log(
                    "debug",
                    &format!("[{key}] 插话采样: 冷却中,还差 {} 条消息", need - since),
                );
                return false;
            }
        }
        // 固定频率:冷却期满即插话,不做概率采样(是否开口交给决策器)
        if ij.mode == "fixed_rate" {
            self.log("debug", &format!("[{key}] 插话: 固定频率命中(每 {need} 条)"));
            self.mark_interjected(key);
            return true;
        }
        let factor = trigger::activity_factor(self.activity_rate(key, ij.activity_window_minutes.max(1)));
        let p = trigger::interject_probability(&cfg, text, factor);
        let rand = pseudo_random();
        let hit = trigger::sample(rand, p);
        self.log(
            "debug",
            &format!(
                "[{key}] 插话采样: 概率 {:.1}%(活跃因子 ×{factor}),随机 {:.4} → {}",
                p * 100.0,
                rand,
                if hit { "🎲 命中" } else { "未命中" }
            ),
        );
        if !hit {
            return false;
        }
        self.mark_interjected(key);
        true
    }

    // ---------- 对外接口(命令层 / GUI) ----------

    /// 会话清理循环:空闲超时移除内存(文件保留)
    pub async fn cleaner_loop(self: Arc<Self>, stop: tokio_util::sync::CancellationToken) {
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    // 轨迹清理:过期条目与空队列(msg_count/interject_at 为累计计数,保留)
                    {
                        let now = SystemTime::now();
                        self.trail.lock().unwrap().retain(|_, q| {
                            q.retain(|(t, _)| {
                                now.duration_since(*t)
                                    .map(|d| d.as_secs() < 86400)
                                    .unwrap_or(false)
                            });
                            !q.is_empty()
                        });
                        self.activity.lock().unwrap().retain(|_, q| !q.is_empty());
                        // 状态归位:空闲会话不保留处理中状态
                        self.status.lock().unwrap().retain(|_, s| {
                            *s == SessionStatus::Idle
                        });
                    }
                    let hours = self.cfg.read().await.chat.clean_after_hours;
                    if hours == 0 { continue; }
                    let limit = Duration::from_secs(hours * 3600);
                    let mut map = self.sessions.write().await;
                    let expired: Vec<String> = map
                        .iter()
                        .filter(|(_, s)| {
                            let idle = s
                                .try_lock()
                                .map(|g| g.last_active.elapsed() > limit)
                                .unwrap_or(false);
                            idle
                        })
                        .map(|(k, _)| k.clone())
                        .collect();
                    for k in expired {
                        map.remove(&k);
                        self.status.lock().unwrap().remove(&k);
                        self.log("info", &format!("[{k}] 会话空闲超过 {hours} 小时,已从内存清理"));
                    }
                    // 串行门随会话一并清理
                    self.msg_gates
                        .write()
                        .await
                        .retain(|k, _| map.contains_key(k));
                }
            }
        }
    }

    /// 供 GUI 使用的会话列表:以磁盘扫描为底(处理中/未加载的会话也能看到),
    /// 内存会话(锁可拿时)补充 token 与摘要状态,状态灯来自状态表。
    pub async fn session_list(&self) -> Vec<serde_json::Value> {
        let mut list = scan_session_files(&self.sessions_dir);
        // 内存补充(锁可拿时覆盖;拿不到则保持文件数据)
        let mut mem: HashMap<String, (usize, u64, bool)> = HashMap::new();
        {
            let map = self.sessions.read().await;
            for (k, s) in map.iter() {
                if let Ok(s) = s.try_lock() {
                    mem.insert(
                        k.clone(),
                        (s.history.len(), s.total_tokens(), s.summary.is_some()),
                    );
                }
            }
        }
        for item in &mut list {
            let key = item["key"].as_str().unwrap_or("").to_string();
            if let Some((count, tokens, has_summary)) = mem.get(&key) {
                item["count"] = json!(count);
                item["tokens"] = json!(tokens);
                item["has_summary"] = json!(has_summary);
            }
            item["status"] = json!(self.get_status(&key).as_str());
        }
        list
    }

    /// 会话详情(轨迹时间线 + 摘要信息 + 进行中回复的直播缓冲);机器人未运行时由命令层直接读盘
    pub async fn session_detail(&self, key: &str) -> serde_json::Value {
        let trace_path = self.trace_dir.join(format!("{key}.jsonl"));
        let events = TraceStore::read_all(&trace_path);
        let file = self.sessions_dir.join(format!("{key}.jsonl"));
        let (count, tokens, has_summary, summary) = read_history_summary(&file);
        let live = self.live.lock().unwrap().get(key).cloned();
        json!({
            "key": key,
            "status": self.get_status(key).as_str(),
            "status_label": self.get_status(key).label(),
            "count": count,
            "tokens": tokens,
            "has_summary": has_summary,
            "summary": summary,
            "events": events,
            "live": live,
        })
    }

/// 改写历史文件中某条消息(按 id 匹配,重估 token);返回 Err 表示未找到。
/// 机器人未运行时命令层也可直接调用(此时无需同步内存会话)。
pub fn rewrite_history_entry(file: &Path, id: &str, text: &str, ratio: f64) -> Result<(), String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("内容不能为空".into());
    }
    let mut found = false;
    let mut out = String::new();
    if let Ok(content) = std::fs::read_to_string(file) {
        for line in content.lines() {
            match serde_json::from_str::<HistoryMsg>(line) {
                Ok(mut h) => {
                    if h.id == id {
                        h.text = text.to_string();
                        h.tokens = estimate_tokens(text, ratio);
                        found = true;
                    }
                    if let Ok(l) = serde_json::to_string(&h) {
                        out.push_str(&l);
                        out.push('\n');
                    }
                }
                Err(_) => {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
    }
    if !found {
        return Err("未找到该消息(可能已折叠进摘要,仅可查看)".into());
    }
    let _ = std::fs::write(file, out);
    Ok(())
}

/// 删除历史文件中某条消息(按 id 匹配);返回 Err 表示未找到。
pub fn remove_history_entry(file: &Path, id: &str) -> Result<(), String> {
    let mut found = false;
    let mut out = String::new();
    if let Ok(content) = std::fs::read_to_string(file) {
        for line in content.lines() {
            match serde_json::from_str::<HistoryMsg>(line) {
                Ok(h) => {
                    if h.id == id {
                        found = true;
                        continue; // 跳过该行 = 删除
                    }
                    if let Ok(l) = serde_json::to_string(&h) {
                        out.push_str(&l);
                        out.push('\n');
                    }
                }
                Err(_) => {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }
    }
    if !found {
        return Err("未找到该消息(可能已折叠进摘要)".into());
    }
    let _ = std::fs::write(file, out);
    Ok(())
}

/// 编辑一条历史消息(同时同步轨迹与内存会话)
pub async fn update_history_msg(&self, key: &str, id: &str, text: &str) -> Result<(), String> {
    let ratio = {
        let cfg = self.cfg.read().await;
        cfg.chat.estimate_ratio
    };
    let file = self.sessions_dir.join(format!("{key}.jsonl"));
    Self::rewrite_history_entry(&file, id, text, ratio)?;
    TraceStore::rewrite_text_by_id(&self.trace_dir.join(format!("{key}.jsonl")), id, text);
    self.reload_session(key).await?;
    self.log("info", &format!("[{key}] 已改写历史消息 {id}"));
    Ok(())
}

/// 删除一条历史消息(同时删除轨迹中对应事件与内存会话)
pub async fn delete_history_msg(&self, key: &str, id: &str) -> Result<(), String> {
    let file = self.sessions_dir.join(format!("{key}.jsonl"));
    Self::remove_history_entry(&file, id)?;
    TraceStore::remove_by_id(&self.trace_dir.join(format!("{key}.jsonl")), id);
    self.reload_session(key).await?;
    self.log("info", &format!("[{key}] 已删除历史消息 {id}"));
    Ok(())
}

    /// 停止进行中的回复(存在运行中的流式请求时生效)
    pub fn stop_session(&self, key: &str) -> bool {
        match self.aborts.lock().unwrap().get(key) {
            Some(tx) => {
                let _ = tx.send(true);
                self.log("info", &format!("[{key}] 收到停止指令"));
                true
            }
            None => false,
        }
    }

    pub async fn clear_session(&self, key: &str) {
        let sess = self.get_session(key).await;
        let locked = sess.try_lock();
        if let Ok(mut s) = locked {
            s.ensure_loaded();
            s.clear();
            // 与 /clear 一致:群聊轨迹一并清空,避免清空后仍注入旧消息
            self.trail.lock().unwrap().remove(key);
            self.log("info", &format!("[{key}] 已清空上下文与群聊轨迹"));
        } else {
            self.log("warn", &format!("[{key}] 会话正在回复中,暂无法清空"));
        }
    }

    /// 清空会话历史轨迹(详情页时间线;上下文历史不动)
    pub fn clear_trace(&self, key: &str) -> Result<(), String> {
        let path = self.trace_dir.join(format!("{key}.jsonl"));
        let _ = std::fs::remove_file(&path);
        self.log("info", &format!("[{key}] 已清空历史轨迹(时间线)"));
        Ok(())
    }
}

/// 轻量伪随机数 [0,1):xorshift 风格混合(避免引入 rand 依赖)
fn pseudo_random() -> f64 {
    static SEED: AtomicU64 = AtomicU64::new(0x9e3779b97f4a7c15);
    let x = SEED
        .fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed)
        .wrapping_mul(0x2545f4914f6cdd1d);
    (x >> 11) as f64 / (1u64 << 53) as f64
}

/// 渲染群聊轨迹为一条 user 消息内容(窗口过滤 + 从最新截断到 max_tokens)。
/// max_tokens == 0 表示不设 token 上限(「全部注入」模式),整段缓冲全部渲染。
pub fn render_trail(
    lines: &VecDeque<(SystemTime, String)>,
    window_secs: u64,
    max_tokens: u32,
    ratio: f64,
) -> Option<String> {
    let now = SystemTime::now();
    let mut rendered: Vec<String> = Vec::new();
    for (t, text) in lines {
        if now
            .duration_since(*t)
            .map(|d| d.as_secs() < window_secs)
            .unwrap_or(false)
        {
            let hhmm = chrono::DateTime::<chrono::Local>::from(*t)
                .format("%H:%M")
                .to_string();
            rendered.push(format!("[{hhmm}] {text}"));
        }
    }
    if rendered.is_empty() {
        return None;
    }
    // 从最新往前保留,直到 token 上限(0 = 无上限)
    let mut total = 0u32;
    let mut kept: Vec<&String> = Vec::new();
    if max_tokens == 0 {
        for line in rendered.iter().rev() {
            kept.push(line);
        }
    } else {
        for line in rendered.iter().rev() {
            total = total.saturating_add(estimate_tokens(line, ratio));
            if total > max_tokens.max(1) {
                break;
            }
            kept.push(line);
        }
    }
    if kept.is_empty() {
        return None;
    }
    let mut content = String::from("[群聊最近消息]\n");
    for line in kept.into_iter().rev() {
        content.push_str(line);
        content.push('\n');
    }
    Some(content)
}

/// 磁盘会话扫描:不依赖内存会话,处理中/未加载的会话也能看到。
/// 供 session_list 与命令层(机器人未运行时)共用。
pub fn scan_session_files(dir: &std::path::Path) -> Vec<serde_json::Value> {
    let mut base: HashMap<String, (usize, u64, bool)> = HashMap::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let path = e.path();
            if path.extension().map(|x| x == "jsonl").unwrap_or(false) {
                let key = e
                    .file_name()
                    .to_string_lossy()
                    .trim_end_matches(".jsonl")
                    .to_string();
                let (count, tokens, has_summary, _) = read_history_summary(&path);
                base.insert(key, (count, tokens, has_summary));
            }
        }
    }
    let mut list: Vec<serde_json::Value> = base
        .into_iter()
        .map(|(key, (count, tokens, has_summary))| {
            json!({
                "key": key,
                "count": count,
                "tokens": tokens,
                "has_summary": has_summary,
            })
        })
        .collect();
    list.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
    list
}

pub fn session_key(msg: &ParsedMsg) -> String {
    match msg.kind {
        MsgKind::Group => format!("g{}", msg.group_id.unwrap_or(0)),
        MsgKind::Private => format!("u{}", msg.user_id),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimate_basic() {
        // 纯 ASCII
        let a = estimate_tokens("hello world this is a test", 1.0);
        assert!(a > 0 && a <= 10);
        // 中文
        let c = estimate_tokens("你好,今天天气怎么样", 1.0);
        assert!(c > 0 && c < 20);
        // 保守系数生效
        let c2 = estimate_tokens("你好,今天天气怎么样", 2.0);
        assert!(c2 > c);
    }

    #[test]
    fn drop_only_head() {
        let toks = [10, 10, 10, 10, 10];
        // 预算 25:丢掉前 3 条(剩 20)
        assert_eq!(compute_drop(&toks, 25, 0), 3);
        // 预算 40:丢 1 条
        assert_eq!(compute_drop(&toks, 40, 0), 1);
        // 预算足够:不丢
        assert_eq!(compute_drop(&toks, 50, 0), 0);
        // min_keep 保护
        assert_eq!(compute_drop(&toks, 0, 2), 3);
        // 单条超大:全丢也要保留 min_keep
        assert_eq!(compute_drop(&[1000], 10, 1), 0);
    }

    #[test]
    fn trail_render() {
        let now = SystemTime::now();
        let lines = vec![
            (now, "你好".to_string()),
            (now, "令牌:abc".to_string()),
        ];
        let out = render_trail(&VecDeque::from(lines.clone()), 300, 800, 1.0).unwrap();
        assert!(out.starts_with("[群聊最近消息]"));
        assert!(out.contains("你好"));
        assert!(out.contains("令牌"));
        assert!(out.contains('[')); // 时间戳格式 [HH:MM]
        // token 上限较小:只保留最新一条(旧的一条放不下)
        let out2 = render_trail(&VecDeque::from(lines.clone()), 300, 10, 1.0).unwrap();
        assert!(out2.contains("令牌"));
        assert!(!out2.contains("你好"));
        // max_tokens = 0:不设上限(「全部注入」模式),整段缓冲全部保留
        let out3 = render_trail(&VecDeque::from(lines), 300, 0, 1.0).unwrap();
        assert!(out3.contains("你好"));
        assert!(out3.contains("令牌"));
        // 过期消息被过滤
        let old = now - Duration::from_secs(600);
        let lines2 = vec![(old, "过期".to_string())];
        assert!(render_trail(&VecDeque::from(lines2), 300, 800, 1.0).is_none());
    }

    #[test]
    fn session_push_persist_roundtrip() {
        let dir = std::env::temp_dir().join("lightbot_test_sessions");
        let _ = std::fs::remove_dir_all(&dir);
        let mut s = Session::new("g123", &dir);
        s.ensure_loaded();
        s.push("user", "你好", 1.0);
        s.push("assistant", "你好呀", 1.0);
        assert_eq!(s.history.len(), 2);

        // 摘要 + 截断后的重写也要落盘(缓存跨重启延续)
        s.summary = Some("用户打了个招呼".into());
        s.summary_tokens = estimate_tokens("用户打了个招呼", 1.15);
        s.rewrite();

        // 重新加载:摘要与历史都在
        let mut s2 = Session::new("g123", &dir);
        s2.ensure_loaded();
        assert_eq!(s2.history.len(), 2);
        assert_eq!(s2.history[0].role, "user");
        assert_eq!(s2.history[0].text, "你好");
        assert_eq!(s2.summary.as_deref(), Some("用户打了个招呼"));
        assert!(s2.summary_tokens > 0);

        s.clear();
        let mut s3 = Session::new("g123", &dir);
        s3.ensure_loaded();
        assert_eq!(s3.history.len(), 0);
        assert!(s3.summary.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn status_labels() {
        assert_eq!(SessionStatus::Idle.as_str(), "idle");
        assert_eq!(SessionStatus::Thinking.label(), "思考中");
    }

    #[test]
    fn history_entry_edit_delete() {
        let dir = std::env::temp_dir().join("lightbot_test_edit");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let file = dir.join("g1.jsonl");
        let mut s = Session::new("g1", &dir);
        s.ensure_loaded();
        s.push_id("user", "你好", 1.0, "id1");
        s.push_id("assistant", "你好呀", 1.0, "id2");
        drop(s);

        // 改写
        ChatCore::rewrite_history_entry(&file, "id1", "改写后的内容", 1.0).unwrap();
        let (count, _, _, _) = read_history_summary(&file);
        assert_eq!(count, 2);
        // 找不到返回 Err
        assert!(ChatCore::rewrite_history_entry(&file, "不存在", "x", 1.0).is_err());

        // 删除
        ChatCore::remove_history_entry(&file, "id1").unwrap();
        let (count, _, _, _) = read_history_summary(&file);
        assert_eq!(count, 1);
        assert!(ChatCore::remove_history_entry(&file, "id1").is_err());

        // 重载后内容一致
        let mut s2 = Session::new("g1", &dir);
        s2.ensure_loaded();
        assert_eq!(s2.history.len(), 1);
        assert_eq!(s2.history[0].text, "你好呀");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
