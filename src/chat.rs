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
    #[serde(skip)]
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
                if let Ok(mut h) = serde_json::from_str::<HistoryMsg>(line) {
                    if h.role == "__summary__" {
                        self.summary_tokens = estimate_tokens(&h.text, 1.15);
                        self.summary = Some(h.text);
                        continue;
                    }
                    h.tokens = 0; // 重估
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
    /// 每群最近一次主动发言时间(插话冷却,软 at 也会刷新)
    pub interject_at: StdMutex<HashMap<String, Instant>>,
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
            interject_at: StdMutex::new(HashMap::new()),
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

        // ① 被动触发(@/回复/关键词/私聊)→ 完整通道(决策器通过才回复)
        if trigger::passive_hit(&cfg, &msg) {
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
            if self.decider_ok(&user_text).await {
                self.log("info", &format!("[{key}] 主动插话: {user_text}"));
                self.light_reply(&msg, &user_text).await;
            } else {
                // 决策拒绝也消耗本次插话机会,防止高频重试
                self.mark_interjected(&key);
                self.log("info", &format!("[{key}] 决策器:无需回复(插话)"));
            }
        }
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
            Ok(yes) => yes,
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
        // [人设][记忆说明] [摘要] [历史] [记忆内容] [提问]
        // 记忆放在历史之后、提问之前:新记忆追加时公共前缀 = 人设+说明+摘要+历史(大头),
        // 缓存几乎全部命中;若记忆在摘要前,新增记忆会让中间消息变长,摘要+历史缓存全断。
        let (prompt, model, mem_cfg) = {
            let cfg = self.cfg.read().await;
            (
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.active_model().cloned(),
                cfg.chat.memory.clone(),
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
        for h in &session.history {
            msgs.push(ApiMessage {
                role: h.role.clone(),
                content: h.text.clone(),
            });
        }
        // 记忆内容消息:位于历史之后、提问之前(追加/删除几乎不影响摘要+历史缓存)
        if mem_cfg.enabled {
            session.memory.refresh();
            if !session.memory.entries.is_empty() {
                msgs.push(ApiMessage {
                    role: "system".into(),
                    content: session.memory.system_text(),
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

        // 思考提示
        if self.cfg.read().await.napcat.reply_pending {
            let pending = self.cfg.read().await.napcat.pending_text.clone();
            let _ = self.send_text(msg, &pending).await;
        }

        // 调 LLM
        let started = Instant::now();
        let max_tokens = {
            let cfg = self.cfg.read().await;
            cfg.chat.reserve_tokens
        };
        match self.llm.chat(&model, &msgs, Some(max_tokens)).await {
            Ok(reply) => {
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
                                if session.memory.remove_contains(&needle) {
                                    self.log("info", &format!("[{key}] 模型删除记忆: {needle}"));
                                }
                            }
                        }
                    }
                }
                if !reply.reasoning.is_empty() {
                    out = format!("💭 {}\n\n{}", reply.reasoning, out);
                }
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
            Err(e) => {
                self.log("error", &format!("[{key}] 模型调用失败: {e}"));
                let _ = self.send_text(msg, &format!("⚠️ 出错了: {e}")).await;
            }
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

    /// 记录群活跃度(滑动窗口,跨度可配置)
    fn track_activity(&self, key: &str, window_secs: u64) {
        let mut m = self.activity.lock().unwrap();
        let q = m.entry(key.to_string()).or_default();
        let now = Instant::now();
        let cutoff = now - Duration::from_secs(window_secs);
        while q.front().map(|t| *t < cutoff).unwrap_or(false) {
            q.pop_front();
        }
        q.push_back(now);
    }

    /// 消息速率(条/分钟):窗口内消息数 / 窗口分钟数
    fn activity_rate(&self, key: &str, window_minutes: u64) -> f64 {
        let m = self.activity.lock().unwrap();
        m.get(key)
            .map(|q| q.len() as f64 / window_minutes.max(1) as f64)
            .unwrap_or(0.0)
    }

    fn mark_interjected(&self, key: &str) {
        self.interject_at
            .lock()
            .unwrap()
            .insert(key.to_string(), Instant::now());
    }

    /// 插话采样:开关 + 冷却 + 概率(基线/钩子词/水消息 + 活跃度缩放)
    async fn interject_sample(&self, key: &str, text: &str) -> bool {
        let cfg = self.cfg.read().await;
        let ij = &cfg.chat.interject;
        if !ij.enabled || ij.mode == "off" {
            return false;
        }
        let cooldown = Duration::from_secs(ij.cooldown_minutes.max(1) * 60);
        let now = Instant::now();
        if let Some(last) = self.interject_at.lock().unwrap().get(key) {
            if now.duration_since(*last) < cooldown {
                return false;
            }
        }
        let factor = trigger::activity_factor(self.activity_rate(key, ij.activity_window_minutes.max(1)));
        let p = trigger::interject_probability(&cfg, text, factor);
        if !trigger::sample(pseudo_random(), p) {
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
                        let _ = self
                            .send_text(
                                msg,
                                if ok { "🧠 已记住。" } else { "这条记忆为空或已存在。" },
                            )
                            .await;
                    }
                    true
                } else if let Some(rest) = text.strip_prefix("/forget ") {
                    let content = rest.trim();
                    if content.is_empty() {
                        let _ = self.send_text(msg, "用法:/forget <内容或序号>").await;
                    } else {
                        session.memory.refresh();
                        let ok = match content.parse::<usize>() {
                            Ok(idx) => session.memory.remove_index(idx),
                            Err(_) => session.memory.remove_contains(content),
                        };
                        let _ = self
                            .send_text(
                                msg,
                                if ok { "🗑️ 已删除该记忆。" } else { "未找到匹配的记忆。" },
                            )
                            .await;
                    }
                    true
                } else if text == "/memories" {
                    session.memory.refresh();
                    if session.memory.entries.is_empty() {
                        let _ = self.send_text(msg, "🧠 暂无记忆。").await;
                    } else {
                        let mut s = String::from("🧠 长期记忆:\n");
                        for (i, e) in session.memory.entries.iter().enumerate() {
                            s.push_str(&format!(
                                "{}. [{}] {}\n",
                                i + 1,
                                if e.source == "model" { "自动" } else { "用户" },
                                e.text
                            ));
                        }
                        let _ = self.send_text(msg, &s).await;
                    }
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
        let (budget, summarize, summarize_tokens, ratio, prompt, mem_cfg) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.context_tokens.saturating_sub(cfg.chat.reserve_tokens) as u64,
                cfg.chat.summarize,
                cfg.chat.summarize_tokens,
                cfg.chat.estimate_ratio,
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.chat.memory.clone(),
            )
        };
        // 固定前缀 token:人设 + 记忆说明(恒定) + 记忆内容(若开启) + 当前提问
        let prompt_tok = (estimate_tokens(&prompt, ratio) + estimate_tokens(MEMORY_GUIDE, ratio))
            as u64;
        let user_tok = estimate_tokens(user_text, ratio) as u64;
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

        // 摘要与历史共享的预算(输入预算扣除人设、记忆与当前提问)
        let input_budget = budget.saturating_sub(prompt_tok + mem_tok + user_tok);
        if input_budget <= sum_tok && session.summary.is_some() {
            session.summary = None;
            sum_tok = 0;
            session.rewrite();
        }
        let mut hist_budget = input_budget.saturating_sub(sum_tok);
        let overflow = hist_tok.saturating_sub(hist_budget);

        if overflow == 0 {
            return;
        }

        if summarize && session.history.len() > 2 {
            // 第一轮:把要丢的部分折叠进摘要(预留摘要空间,并至少丢 2 条)
            let drop = compute_drop(
                &session.history.iter().map(|h| h.tokens).collect::<Vec<_>>(),
                hist_budget,
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
        for (i, chunk) in chunks.iter().enumerate() {
            match msg.kind {
                MsgKind::Group => {
                    self.sender
                        .send_group_msg(msg.group_id.unwrap_or(0), chunk)
                        .await?;
                }
                MsgKind::Private => {
                    self.sender.send_private_msg(msg.user_id, chunk).await?;
                }
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
                    // 插话状态清理:保留一天内的冷却记录,活跃度只留非空队列
                    {
                        let now = Instant::now();
                        let day = Duration::from_secs(86400);
                        self.interject_at
                            .lock()
                            .unwrap()
                            .retain(|_, t| now.duration_since(*t) < day);
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

    /// 供 GUI 使用的会话列表
    pub async fn session_list(&self) -> Vec<serde_json::Value> {
        let map = self.sessions.read().await;
        let mut list = Vec::new();
        for (k, s) in map.iter() {
            if let Ok(s) = s.try_lock() {
                list.push(json!({
                    "key": k,
                    "count": s.history.len(),
                    "tokens": s.total_tokens(),
                    "has_summary": s.summary.is_some(),
                }));
            }
        }
        list.sort_by(|a, b| a["key"].as_str().cmp(&b["key"].as_str()));
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
