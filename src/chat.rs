//! 会话与上下文管理 —— 缓存优化核心。
//!
//! 缓存友好原则(针对 DeepSeek 自动 context caching):
//! 1. 稳定前缀:system(人设)固定在最前,摘要紧随其后,历史消息只追加、从不改写;
//! 2. 头部截断:超出预算时只从最旧消息开始丢弃,剩余部分保持逐字节不变 → 前缀哈希命中;
//! 3. 摘要折叠:需要大量丢弃时,把丢弃部分用 LLM 压缩成固定摘要插入前缀区,之后继续追加;
//! 4. 会话持久化(JSONL):重启后同一会话继续追加,缓存可跨重启延续。
//!
//! 每个会话串行处理(try_lock 防堆积),token 估算轻量(字符级,可配保守系数)。

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::config::Config;
use crate::llm::{ApiMessage, LlmClient};
use crate::memory::{self, MemoryOp, MemoryStore};
use crate::napcat::{ActionSender, ConnStatus, MsgKind, ParsedMsg};
use crate::trigger;

// ---------- 前端事件 ----------

/// 记忆管理说明:恒定的独立 system 消息(与开关无关,开关切换不影响缓存前缀)。
/// ⚠️ 此文本内容改动会破坏缓存前缀,勿随意修改。
pub const MEMORY_GUIDE: &str = "(你可以管理长期记忆:当你了解到值得长期记住的信息(用户偏好、重要事实、约定)时,在回复末尾用标记 [记忆:添加 内容] 写入;需要删除时用 [记忆:删除 内容片段]。不要写入临时性信息,每次只写最重要的。)";

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
            memory: MemoryStore::new(
                dir.join("memories").join(format!("{key}.jsonl")),
            ),
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
                if let Ok(h) = serde_json::from_str::<HistoryMsg>(line) {
                    if h.role == "__summary__" {
                        self.summary_tokens = estimate_tokens(&h.text, 1.15);
                        self.summary = Some(h.text);
                        continue;
                    }
                    self.history.push(h);
                }
            }
        }
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

    /// 追加一条并落盘
    fn push(&mut self, role: &str, text: &str, ratio: f64) {
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

// ---------- 业务核心 ----------

pub struct ChatCore {
    pub cfg: Arc<RwLock<Config>>,
    pub llm: LlmClient,
    pub sender: ActionSender,
    pub sessions: RwLock<HashMap<String, Arc<Mutex<Session>>>>,
    pub sessions_dir: PathBuf,
    /// 配置文件路径(/model、/prompt 命令切换后自动保存)
    pub cfg_path: PathBuf,
    pub events: mpsc::Sender<FrontendEvent>,
    /// 群活跃度跟踪:key -> 最近 5 分钟消息时间戳(插话采样用)
    pub activity: StdMutex<HashMap<String, VecDeque<Instant>>>,
    /// 每群消息计数(插话条数冷却)
    pub msg_count: StdMutex<HashMap<String, u64>>,
    /// 每群最近一次主动发言时的消息计数(插话冷却,软 at 也会刷新)
    pub interject_at: StdMutex<HashMap<String, u64>>,
    /// 群聊轨迹:未触发对话的普通消息缓冲(key -> (时间, 文本))
    pub trail: StdMutex<HashMap<String, VecDeque<(SystemTime, String)>>>,
}

impl ChatCore {
    pub fn new(
        cfg: Arc<RwLock<Config>>,
        sender: ActionSender,
        sessions_dir: PathBuf,
        cfg_path: PathBuf,
        events: mpsc::Sender<FrontendEvent>,
    ) -> Self {
        Self {
            cfg,
            llm: LlmClient::new(),
            sender,
            sessions: RwLock::new(HashMap::new()),
            sessions_dir,
            cfg_path,
            events,
            activity: StdMutex::new(HashMap::new()),
            msg_count: StdMutex::new(HashMap::new()),
            interject_at: StdMutex::new(HashMap::new()),
            trail: StdMutex::new(HashMap::new()),
        }
    }

    fn log(&self, level: &str, msg: &str) {
        let _ = self.events.try_send(FrontendEvent::Log {
            level: level.to_string(),
            msg: msg.to_string(),
        });
    }

    /// 事件入口:消息分流 —— 被动触发/软 at 走完整通道,插话采样走轻量通道
    pub async fn handle_message(&self, msg: ParsedMsg) {
        if msg.is_self {
            return;
        }
        let key = session_key(&msg);
        self.emit_msg_in(&key, &msg.text);

        let cfg = self.cfg.read().await.clone();
        // 活跃度窗口(可配置,默认 2 分钟)
        let win_min = cfg.chat.interject.activity_window_minutes.max(1);
        self.track_activity(&key, win_min * 60);

        // ① 被动触发(@/回复/关键词/私聊)→ 完整通道;群聊中 / 开头的命令消息也直接触发
        let is_cmd = msg.kind == MsgKind::Group && msg.text.trim_start().starts_with('/');
        if trigger::passive_hit(&cfg, &msg) || is_cmd {
            drop(cfg);
            if self.decider_ok(&msg.text).await {
                self.full_dialogue(&key, &msg).await;
            } else {
                self.log("info", &format!("[{key}] 决策器:无需回复(被动触发)"));
            }
            return;
        }
        // ② 软 at(提到机器人称呼)→ 完整通道,必回,刷新插话冷却(决策器通过才回复)
        if msg.kind == MsgKind::Group && trigger::soft_at_hit(&cfg, &msg.text) {
            drop(cfg);
            if self.decider_ok(&msg.text).await {
                self.mark_interjected(&key);
                self.log("info", &format!("[{key}] 软 at 触发(称呼提及)"));
                self.full_dialogue(&key, &msg).await;
            } else {
                self.log("info", &format!("[{key}] 决策器:无需回复(软 at)"));
            }
            return;
        }
        // ③ 插话采样 → 轻量通道(群聊,概率 + 冷却;决策器通过才插话)
        let user_text = trigger::strip_keyword(&msg.text, &cfg.napcat.keyword).to_string();
        if msg.kind == MsgKind::Group
            && !user_text.is_empty()
            && self.interject_sample(&key, &user_text).await
        {
            drop(cfg);
            // 插话消息未进历史,记录到轨迹
            self.record_trail(&key, &msg.text).await;
            if self.decider_ok(&user_text).await {
                self.log("info", &format!("[{key}] 主动插话: {user_text}"));
                self.light_reply(&msg, &user_text).await;
            } else {
                // 决策拒绝也消耗本次插话机会,防止高频重试
                self.mark_interjected(&key);
                self.log("info", &format!("[{key}] 决策器:无需回复(插话)"));
            }
        } else {
            // 全部触发条件未命中:记录群聊轨迹(解决"鱼的记忆")
            self.log(
                "debug",
                &format!("[{key}] 未触发回复逻辑: {}", msg.text),
            );
            self.record_trail(&key, &msg.text).await;
        }
    }

    /// 记录群聊轨迹(仅群聊;窗口与条数取自配置)
    async fn record_trail(&self, key: &str, text: &str) {
        if !key.starts_with('g') {
            return;
        }
        let (win, max) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.trail.window_minutes.max(1) * 60,
                cfg.chat.trail.max_entries as usize,
            )
        };
        self.trail_push(key, text, win, max);
    }

    /// 决策器:开启时由当前模型判断这条消息是否需要回复。
    /// 关闭 / 无模型 / 决策调用失败 → 按需要回复处理(保守,不漏回消息)。
    async fn decider_ok(&self, text: &str) -> bool {
        let cfg = self.cfg.read().await;
        if !cfg.chat.decider {
            return true;
        }
        let model = cfg.active_model().cloned();
        let prompt = cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default();
        drop(cfg);
        let Some(model) = model else {
            return true;
        };
        match self.llm.decide(&model, &prompt, text).await {
            Ok(yes) => {
                self.log(
                    "info",
                    &format!("决策器: {}这条消息", if yes { "需要回复" } else { "无需回复" }),
                );
                yes
            }
            Err(e) => {
                self.log("warn", &format!("决策器调用失败,按需要回复处理: {e}"));
                true
            }
        }
    }

    /// 完整通道:会话级对话(命令 / 上下文预算 / 历史 / 落盘)
    async fn full_dialogue(&self, key: &str, msg: &ParsedMsg) {
        // 会话内串行:busy 时忽略(日志记录)
        let sess = self.get_session(key).await;
        let Ok(mut session) = sess.try_lock() else {
            self.log("warn", &format!("[{key}] 上一条消息仍在处理,忽略本条"));
            return;
        };
        session.ensure_loaded();
        session.last_active = Instant::now();
        self.log(
            "debug",
            &format!("[{key}] 会话加载: 历史 {} 条", session.history.len()),
        );

        // 命令处理
        let text = msg.text.trim();
        if text.starts_with('/') {
            if self.handle_command(&mut session, msg, text).await {
                return;
            }
        }

        // 群聊触发时,剥离关键词前缀
        let user_text = {
            let cfg = self.cfg.read().await;
            trigger::strip_keyword(text, &cfg.napcat.keyword).to_string()
        };
        if user_text.is_empty() {
            return;
        }

        // 上下文预算管理(缓存友好截断/摘要)
        self.trim_context(&mut session, &user_text).await;

        // 组装消息(缓存友好顺序):
        // [人设][记忆说明][摘要][记忆(front)][历史][记忆(back)][轨迹][提问]
        // 记忆位置由配置决定:front = 摘要后/历史前(记忆与历史都命中,记忆变更断历史一次);
        // back = 历史后(记忆变更不影响历史,但历史增长使记忆每轮 miss)。
        // 轨迹(未触发消息)总在最后,变化只影响自身与提问。
        let (prompt, model, mem_cfg, trail_cfg, ratio) = {
            let cfg = self.cfg.read().await;
            (
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.active_model().cloned(),
                cfg.chat.memory.clone(),
                cfg.chat.trail.clone(),
                cfg.chat.estimate_ratio,
            )
        };
        let Some(model) = model else {
            self.log("error", "未配置可用模型");
            return;
        };
        // 记忆管理说明作为恒定独立 system 消息(与人设分离):
        // 开关切换不改变消息流前缀 → 缓存不受影响;关闭时仅不展示记忆内容、不执行标记
        let mut msgs = vec![
            ApiMessage {
                role: "system".into(),
                content: prompt,
            },
            ApiMessage {
                role: "system".into(),
                content: MEMORY_GUIDE.to_string(),
            },
        ];
        if let Some(s) = &session.summary {
            msgs.push(ApiMessage {
                role: "system".into(),
                content: format!("[先前对话摘要]\n{s}"),
            });
        }
        // 记忆内容:按 placement 决定插在摘要后(历史前)还是历史后
        let mut mem_msg: Option<ApiMessage> = None;
        if mem_cfg.enabled {
            session.memory.refresh();
            self.log(
                "debug",
                &format!("[{key}] 记忆: {} 条", session.memory.entries.len()),
            );
            if !session.memory.entries.is_empty() {
                mem_msg = Some(ApiMessage {
                    role: "system".into(),
                    content: session.memory.system_text(),
                });
            }
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
        // 群聊轨迹:最近未触发消息(位于最后,变化不影响历史/记忆缓存)
        if trail_cfg.enabled && msg.kind == MsgKind::Group {
            let lines = {
                self.trail
                    .lock()
                    .unwrap()
                    .get(key)
                    .cloned()
                    .unwrap_or_default()
            };
            if let Some(content) = render_trail(
                &lines,
                trail_cfg.window_minutes.max(1) * 60,
                trail_cfg.max_tokens,
                ratio,
            ) {
                msgs.push(ApiMessage {
                    role: "user".into(),
                    content,
                });
            }
        }
        msgs.push(ApiMessage {
            role: "user".into(),
            content: user_text.clone(),
        });

        // 先存用户消息(即使失败也在历史里)
        {
            let cfg = self.cfg.read().await;
            let ratio = cfg.chat.estimate_ratio;
            session.push("user", &user_text, ratio);
            self.emit_session(key, &session).await;
            drop(cfg);
        }

        // 调 LLM:思考提示改为延迟触发 —— 仅当思考超过阈值仍未返回才发提示,
        // 避免一开始就发一条突兀的消息;首 token 到达前的等待(约 3-5s)
        // 不计入思考时间,按 4s 估计扣除(非流式请求无法精确获知首 token 时刻)
        let started = Instant::now();
        let (pending_enabled, pending_text, pending_delay) = {
            let cfg = self.cfg.read().await;
            (
                cfg.napcat.reply_pending,
                cfg.napcat.pending_text.clone(),
                cfg.napcat.pending_delay_secs,
            )
        };
        let max_tokens = {
            let cfg = self.cfg.read().await;
            cfg.chat.reserve_tokens
        };
        let reply = if pending_enabled {
            let chat_fut = self.llm.chat(&model, &msgs, Some(max_tokens));
            tokio::pin!(chat_fut);
            let threshold = Duration::from_secs(pending_delay.saturating_add(4));
            let mut prompted = false;
            let result = loop {
                tokio::select! {
                    r = &mut chat_fut => break Some(r),
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        if !prompted && started.elapsed() >= threshold {
                            let _ = self.send_text(msg, &pending_text).await;
                            prompted = true;
                        }
                    }
                }
            };
            result
        } else {
            Some(self.llm.chat(&model, &msgs, Some(max_tokens)).await)
        };
        match reply {
            Some(Ok(reply)) => {
                let elapsed = started.elapsed().as_millis() as u64;
                let _ = self.events.try_send(FrontendEvent::LlmStats {
                    model: model.model.clone(),
                    prompt_tokens: reply.usage.prompt_tokens,
                    completion_tokens: reply.usage.completion_tokens,
                    cache_hit: reply.usage.cache_hit,
                    cache_miss: reply.usage.cache_miss,
                    reasoning_tokens: reply.usage.reasoning_tokens,
                    elapsed_ms: elapsed,
                });
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
                let mut out = reply.text;
                // 记忆标记:总是剥离(防止关闭状态泄漏到群里);仅开启时执行
                let (clean, ops) = memory::parse_memory_ops(&out);
                out = clean;
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
                                    self.log("info", &format!("[{key}] 模型写入记忆: {text}"));
                                }
                            }
            MemoryOp::Remove(needle) => {
                                if session.memory.remove_contains(&needle) > 0 {
                                    self.log("info", &format!("[{key}] 模型删除记忆: {needle}"));
                                }
                            }
                        }
                    }
                }
                // 思考过程(reasoning_content)只用于统计,绝不发送给用户
                if out.is_empty() {
                    out = "(模型未返回内容)".into();
                }
                let _ = self.send_text(msg, &out).await;
                {
                    let cfg = self.cfg.read().await;
                    let ratio = cfg.chat.estimate_ratio;
                    session.push("assistant", &out, ratio);
                    self.emit_session(key, &session).await;
                }
            }
            Some(Err(e)) => {
                self.log("error", &format!("[{key}] 模型调用失败: {e}"));
                let _ = self.send_text(msg, &format!("⚠️ 出错了: {e}")).await;
            }
            None => unreachable!(),
        }
    }

    /// 轻量通道:单轮插话(极小上下文、不落盘、不新开会话、失败静默)
    async fn light_reply(&self, msg: &ParsedMsg, user_text: &str) {
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
        let msgs = vec![
            ApiMessage {
                role: "system".into(),
                content: format!(
                    "{prompt}\n\n(现在是群聊中的随口插话场景:请用一两句话自然、口语化地回应,不要称呼任何人,不要使用列表或长篇大论。)"
                ),
            },
            ApiMessage {
                role: "user".into(),
                content: user_text.to_string(),
            },
        ];
        match self.llm.chat(&model, &msgs, Some(max_tokens)).await {
            Ok(reply) => {
                // 插话场景剥离记忆标记但不执行(轻量通道不管理记忆)
                let (text, _) = memory::parse_memory_ops(&reply.text);
                let out = if text.is_empty() {
                    "(模型未返回内容)".to_string()
                } else {
                    text
                };
                let _ = self.send_text(msg, &out).await;
            }
            Err(e) => {
                // 插话失败静默,不打扰群
                self.log("warn", &format!("[{}] 插话失败: {e}", session_key(msg)));
            }
        }
    }

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

    /// 记录群聊轨迹(未触发对话的消息),按窗口与条数限制
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
        while q.len() > max_entries.max(1) {
            q.pop_front();
        }
    }

    /// 插话采样:开关 + 冷却 + 概率(基线/钩子词/水消息 + 活跃度缩放)
    async fn interject_sample(&self, key: &str, text: &str) -> bool {
        let cfg = self.cfg.read().await;
        let ij = &cfg.chat.interject;
        if !ij.enabled || ij.mode == "off" {
            return false;
        }
        // 冷却:距上次主动发言以来,群里新消息达到 cooldown_messages 条才允许插话
        let count = self.msg_count.lock().unwrap().get(key).copied().unwrap_or(0);
        let need = ij.cooldown_messages.max(1) as u64;
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

    /// 内置斜杠命令,返回 true 表示已处理
    async fn handle_command(&self, session: &mut Session, msg: &ParsedMsg, text: &str) -> bool {
        let cfg = self.cfg.read().await;
        match text {
            "/clear" => {
                session.clear();
                self.emit_session(&session.key, session).await;
                self.log("info", &format!("[{}] 已清空上下文", session.key));
                let _ = self
                    .send_text(msg, "🧹 上下文已清空,重新开始对话。")
                    .await;
                true
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
                let _ = self
                    .send_text(
                        msg,
                        &format!(
                            "📊 当前会话:{} 条消息,约 {} tokens{}",
                            session.history.len(),
                            toks,
                            session
                                .summary
                                .as_ref()
                                .map(|_| "(含摘要)")
                                .unwrap_or("")
                        ),
                    )
                    .await;
                true
            }
            _ => {
                if let Some(rest) = text.strip_prefix("/remember ") {
                    let content = rest.trim();
                    if content.is_empty() {
                        let _ = self.send_text(msg, "用法:/remember <内容>").await;
                    } else {
                        session.memory.refresh();
                        let ok = session.memory.add(
                            content,
                            "user",
                            cfg.chat.memory.max_entries as usize,
                            cfg.chat.memory.max_entry_chars as usize,
                        );
                        let reply_text = if ok {
                            format!("🧠 已记住: {content}")
                        } else {
                            "这条记忆为空或已存在。".to_string()
                        };
                        self.log(
                            "info",
                            &format!(
                                "[{}] 用户添加记忆{}: {content}",
                                session.key,
                                if ok { "" } else { "(重复或为空)" }
                            ),
                        );
                        let _ = self.send_text(msg, &reply_text).await;
                    }
                    true
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
                        let _ = self
                            .send_text(msg, "用法:/forget <序号,逗号分隔,如 1,3>")
                            .await;
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
                        let reply_text = if removed > 0 {
                            format!("🗑️ 已删除 {removed} 条记忆。")
                        } else {
                            "未找到匹配的序号。".to_string()
                        };
                        self.log(
                            "info",
                            &format!(
                                "[{}] 用户删除记忆: 序号 [{}],删除 {removed} 条",
                                session.key,
                                content
                            ),
                        );
                        let _ = self.send_text(msg, &reply_text).await;
                    }
                    true
                } else if text == "/memories" {
                    session.memory.refresh();
                    if session.memory.entries.is_empty() {
                        let _ = self.send_text(msg, "🧠 暂无记忆。").await;
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
                        let _ = self.send_text(msg, &s).await;
                    }
                    true
                } else if text == "/model" {
                    let list: Vec<String> = cfg.models.iter().map(|m| m.name.clone()).collect();
                    let _ = self
                        .send_text(msg, &format!("可用模型: {}", list.join(", ")))
                        .await;
                    true
                } else if let Some(rest) = text.strip_prefix("/model ") {
                    let name = rest.trim();
                    if cfg.models.iter().any(|m| m.name == name) {
                        {
                            let mut cfg = self.cfg.write().await;
                            cfg.active_model = name.to_string();
                        }
                        let cfg = self.cfg.read().await;
                        let _ = crate::config::save_config(&self.cfg_path, &cfg);
                        self.log("info", &format!("[{}] 切换模型 -> {name}", session.key));
                        let _ = self
                            .send_text(msg, &format!("✅ 已切换模型: {name}"))
                            .await;
                    } else {
                        let list: Vec<String> = cfg.models.iter().map(|m| m.name.clone()).collect();
                        let _ = self
                            .send_text(msg, &format!("❌ 未找到模型 {name},可用: {}", list.join(", ")))
                            .await;
                    }
                    true
                } else if text == "/prompt" {
                    let list: Vec<String> = cfg
                        .prompts
                        .iter()
                        .map(|p| format!("{} ({})", p.name, p.id))
                        .collect();
                    let _ = self
                        .send_text(msg, &format!("可用人设: {}", list.join(", ")))
                        .await;
                    true
                } else if let Some(rest) = text.strip_prefix("/prompt ") {
                    let id = rest.trim();
                    if cfg.prompts.iter().any(|p| p.id == id) {
                        {
                            let mut cfg = self.cfg.write().await;
                            cfg.active_prompt = id.to_string();
                        }
                        let cfg = self.cfg.read().await;
                        let _ = crate::config::save_config(&self.cfg_path, &cfg);
                        self.log("info", &format!("[{}] 切换人设 -> {id}", session.key));
                        let _ = self
                            .send_text(msg, &format!("✅ 已切换人设: {id}"))
                            .await;
                    } else {
                        let list: Vec<String> = cfg.prompts.iter().map(|p| p.id.clone()).collect();
                        let _ = self
                            .send_text(msg, &format!("❌ 未找到人设 {id},可用: {}", list.join(", ")))
                            .await;
                    }
                    true
                } else {
                    false
                }
            }
        }
    }

    /// 上下文预算管理:缓存友好截断 + 可选摘要折叠
    async fn trim_context(&self, session: &mut Session, user_text: &str) {
        let (budget, summarize, summarize_tokens, ratio, prompt, mem_cfg, history_target) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.context_tokens.saturating_sub(cfg.chat.reserve_tokens) as u64,
                cfg.chat.summarize,
                cfg.chat.summarize_tokens,
                cfg.chat.estimate_ratio,
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.chat.memory.clone(),
                cfg.chat.history_target_tokens,
            )
        };
        let (prompt_tok, user_tok) = {
            // 固定前缀 token:人设 + 记忆说明(恒定) + 当前提问
            let pt = (estimate_tokens(&prompt, ratio) + estimate_tokens(MEMORY_GUIDE, ratio))
                as u64;
            (pt, estimate_tokens(user_text, ratio) as u64)
        };
        // 记忆:刷新 + 超预算裁剪最旧条目(保护上下文预算)
        let mut mem_tok: u64 = 0;
        if mem_cfg.enabled {
            session.memory.refresh();
            session
                .memory
                .trim_to_tokens(mem_cfg.max_tokens.max(64), ratio);
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
        let mut hist_budget = input_budget.saturating_sub(sum_tok);
        let overflow = hist_tok.saturating_sub(hist_budget);

        // 主动折叠目标:history_target_tokens(>0 时启用,配合记忆 front 策略保持缓存最优)
        let fold_budget = if history_target > 0 {
            hist_budget.min(history_target as u64)
        } else {
            hist_budget
        };
        let need_fold =
            summarize && session.history.len() > 2 && (hist_tok > fold_budget || overflow > 0);
        if !need_fold {
            return;
        }

        if summarize && session.history.len() > 2 {
            // 第一轮:把要丢的部分折叠进摘要(预留摘要空间,并至少丢 2 条)
            let drop = compute_drop(
                &session.history.iter().map(|h| h.tokens).collect::<Vec<_>>(),
                fold_budget,
                1,
            )
            .max(2);
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
            if let Some(model) = model {
                match self
                    .llm
                    .summarize(&model, &old_summary, &api_msgs, summarize_tokens)
                    .await
                {
                    Ok(s) => {
                        session.summary = Some(s.clone());
                        session.summary_tokens = estimate_tokens(&s, ratio);
                        session.rewrite();
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
                }
            } else {
                // 无可用模型:丢弃部分也需落盘,保证磁盘与内存一致
                session.rewrite();
                self.log(
                    "info",
                    &format!("[{}] 无可用模型,直接丢弃最旧 {} 条消息", session.key, drop),
                );
            }
            sum_tok = session.summary_tokens as u64;
            hist_budget = budget
                .saturating_sub(prompt_tok + user_tok)
                .saturating_sub(sum_tok);
        }

        // 第二轮:直接头部截断(保证不超预算,只删最旧)
        let drop = compute_drop(
            &session.history.iter().map(|h| h.tokens).collect::<Vec<_>>(),
            hist_budget,
            0,
        );
        if drop > 0 {
            session.history.drain(..drop);
            session.rewrite();
            self.log(
                "info",
                &format!("[{}] 上下文超预算,丢弃最旧 {} 条消息", session.key, drop),
            );
        }
    }

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
                MsgKind::Private => {
                    self.sender.send_private_msg(msg.user_id, chunk).await
                }
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
        let _ = self.events.try_send(FrontendEvent::MsgIn {
            key: key.to_string(),
            text: text.to_string(),
        });
    }

    fn emit_msg_out(&self, key: &str, text: &str) {
        let _ = self.events.try_send(FrontendEvent::MsgOut {
            key: key.to_string(),
            text: text.to_string(),
        });
    }

    async fn emit_session(&self, key: &str, session: &Session) {
        let _ = self.events
            .try_send(FrontendEvent::SessionChanged {
                key: key.to_string(),
                count: session.history.len(),
                tokens: session.total_tokens(),
            });
    }

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
                        self.log("info", &format!("[{k}] 会话空闲超过 {hours} 小时,已从内存清理"));
                    }
                }
            }
        }
    }

    /// 供 GUI 使用的会话列表:以磁盘扫描为底(处理中/未加载的会话也能看到),
    /// 内存会话(锁可拿时)补充 token 与摘要状态。
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
            let key = item["key"].as_str().unwrap_or("");
            if let Some((count, tokens, has_summary)) = mem.get(key) {
                item["count"] = json!(count);
                item["tokens"] = json!(tokens);
                item["has_summary"] = json!(has_summary);
            }
        }
        list
    }

    pub async fn clear_session(&self, key: &str) {
        let sess = self.get_session(key).await;
        let locked = sess.try_lock();
        if let Ok(mut s) = locked {
            s.ensure_loaded();
            s.clear();
            self.log("info", &format!("[{key}] 已清空上下文"));
        }
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

/// 渲染群聊轨迹为一条 user 消息内容(窗口过滤 + 从最新截断到 max_tokens)
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
    // 从最新往前保留,直到 token 上限
    let mut total = 0u32;
    let mut kept: Vec<&String> = Vec::new();
    for line in rendered.iter().rev() {
        total = total.saturating_add(estimate_tokens(line, ratio));
        if total > max_tokens.max(1) {
            break;
        }
        kept.push(line);
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
                let mut count = 0usize;
                let mut tokens = 0u64;
                let mut has_summary = false;
                if let Ok(content) = std::fs::read_to_string(&path) {
                    for line in content.lines() {
                        count += 1;
                        if !has_summary {
                            if let Ok(h) = serde_json::from_str::<HistoryMsg>(line) {
                                if h.role == "__summary__" {
                                    has_summary = true;
                                    continue;
                                }
                            }
                        }
                        tokens += serde_json::from_str::<HistoryMsg>(line)
                            .map(|h| h.tokens as u64)
                            .unwrap_or(0);
                    }
                }
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
        let out2 = render_trail(&VecDeque::from(lines), 300, 10, 1.0).unwrap();
        assert!(out2.contains("令牌"));
        assert!(!out2.contains("你好"));
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
}
