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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::{watch, Mutex, RwLock};

use crate::config::{Config, ModelConfig};
use crate::cost::{CostTracker, UsageRecord};
use crate::events::{EventBuf, FrontendEvent, SessionStatus};
use crate::llm::{ApiMessage, LlmClient, StreamEvent, Usage, USER_STOPPED};
use crate::memory::{self, MemoryOp};
use crate::napcat::{ActionSender, MsgKind, ParsedMsg};
use crate::placement::{self, Eval, PlacementController, Prices, Scheme};
use crate::runtime::SessionRegistry;
use crate::session::{
    read_history_summary, remove_history_entry, rewrite_history_entry, scan_session_files,
    session_key, Session,
};
use crate::trace::{self, TraceEvent, TraceStore};
use crate::context::speaker_prefix;
use crate::trigger;
use crate::trigger::ignore_prefix_hit;

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
    /// 会话运行时状态(活跃度/插话冷却/轨迹/状态灯/中止/排队/直播缓冲,统一注册表)
    pub rt: SessionRegistry,
    /// 全局暂停回复(与命令层共享):true 时只接收消息,不决策/不回复/不思考
    pub paused: Arc<AtomicBool>,
    /// 决策器全局中止通道(暂停时取消进行中的决策请求)
    pub decide_cancel: watch::Sender<bool>,
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
            rt: SessionRegistry::new(),
            cost,
            placement,
            paused,
            decide_cancel,
            mem_changes: AtomicU64::new(0),
        }
    }

    pub(crate) fn log(&self, level: &str, msg: &str) {
        self.events.lock().unwrap().push(FrontendEvent::Log {
            level: level.to_string(),
            msg: msg.to_string(),
        });
    }

    /// 会话状态变化:注册表更新 + 事件入环形缓冲(前端经 get_events 拉取)
    async fn set_status(&self, key: &str, s: SessionStatus) {
        if let Some(s) = self.rt.set_status(key, s) {
            self.events.lock().unwrap().push(FrontendEvent::SessionStatus {
                key: key.to_string(),
                status: s.as_str().to_string(),
            });
        }
    }

    pub fn get_status(&self, key: &str) -> SessionStatus {
        self.rt.get_status(key)
    }

    /// 每会话消息串行门(跨会话并行:同群保序,不同群互不阻塞)
    async fn get_gate(&self, key: &str) -> Arc<Mutex<()>> {
        self.rt.gate(key).await
    }

    /// 全局暂停:中止所有进行中的流式回复与决策请求、清空排队。
    /// 暂停后消息仍接收/记录,但不再决策与回复;resume_processing 恢复。
    pub fn stop_all_processing(&self) {
        self.paused.store(true, Ordering::Relaxed);
        self.rt.abort_send_all();
        self.rt.pending_clear();
        let _ = self.decide_cancel.send(true);
    }

    pub fn resume_processing(&self) {
        let _ = self.decide_cancel.send(false);
        self.paused.store(false, Ordering::Relaxed);
    }

    // ---------- 轨迹与费用 ----------

    pub(crate) async fn trace_push(&self, key: &str, ev: &TraceEvent) {
        self.events.lock().unwrap().push(FrontendEvent::Trace {
            key: key.to_string(),
            entry: ev.clone(),
        });
        let store = TraceStore::new(self.trace_dir.join(format!("{key}.jsonl")));
        store.push(ev);
    }

    pub(crate) fn record_usage(&self, model: &ModelConfig, category: &str, usage: &Usage) {
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
        // ① 入口拦截:忽略前缀命中时,无论 @/引用/关键词 一律不处理(仅记录)。
        //    剥掉开头的引用占位符再判断,引用消息与关键词触发同样被挡。
        let ignored = {
            let cfg = self.cfg.read().await;
            ignore_prefix_hit(&cfg, &msg.text)
        };
        if ignored {
            let turn = trace::new_id();
            self.emit_msg_in(&key, &msg.text);
            self.log("info", &format!("[{key}] 忽略前缀消息,不处理: {}", msg.text));
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

        // 会话级暂停(/pause 设置):仅本会话只接收,不决策、不回复、不思考。
        // 只放行 /pause 与 /resume 本身——否则暂停后无法恢复,且未知斜杠命令
        // 会借道命令通道触发 LLM 对话,破坏「仅接收」语义。
        // (剥掉引用占位符再判断:引用回复机器人发出的 /resume 同样有效)
        {
            let t = trigger::strip_quote_prefix(msg.text.trim());
            let pause_ctrl = t == "/pause" || t == "/resume";
            if self.rt.is_session_paused(&key) && !pause_ctrl {
                self.log(
                    "info",
                    &format!("[{key}] 本会话已暂停回复(/pause),仅接收: {}", msg.text),
                );
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
        }

        let cfg = self.cfg.read().await.clone();
        // 活跃度窗口(可配置,默认 2 分钟)
        let win_min = cfg.chat.interject.activity_window_minutes.max(1);
        self.rt.track_activity(&key, win_min * 60);

        // 斜杠命令:群聊/私聊均可直接触发,跳过决策器(命令绝不因决策器被吞)。
        // 引用回复机器人的消息剥掉占位符后同样识别为命令。
        let is_cmd = trigger::strip_quote_prefix(msg.text.trim_start()).starts_with('/');
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
            if self.decider_ok(&key, &turn, &msg.text, hint, msg.user_id).await {
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
            if self.decider_ok(&key, &turn, &msg.text, "消息提到了你的称呼", msg.user_id).await {
                self.rt.mark_interjected(&key);
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
            if self.decider_ok(&key, &turn, &user_text, "主动插话采样命中(是否接话)", msg.user_id).await {
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
                    self.record_trail(&key, &msg.text, msg.user_id).await;
                    self.light_reply(&msg, &user_text, &turn).await;
                }
            } else {
                // 决策拒绝也消耗本次插话机会,防止高频重试
                self.rt.mark_interjected(&key);
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
            self.record_trail(&key, &msg.text, msg.user_id).await;
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
    async fn record_trail(&self, key: &str, text: &str, sender: i64) {
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
        self.rt.trail_push(key, text, sender, window_secs, max_entries);
    }

    /// 决策器:开启时由当前模型判断这条消息是否需要回复。
    /// 关闭 / 无模型 / 决策调用失败 → 按需要回复处理(保守,不漏回消息)。
    /// trigger_hint 说明消息的触发方式(修复:决策器此前看不到 @ 信息,把召唤消息误判为闲聊)
    async fn decider_ok(&self, key: &str, turn: &str, text: &str, trigger_hint: &str, sender: i64) -> bool {
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
            r = self.llm.decide(&model, &prompt, text, trigger_hint, sender) => Some(r),
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

        // 命令处理(先于一切模型调用;返回 Some(回复) 表示已处理)。
        // 剥掉引用占位符:引用回复机器人的消息带命令同样执行。
        let text = trigger::strip_quote_prefix(msg.text.trim());
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

        // 群聊触发时,剥离关键词前缀;剥完为空(只 @ 没说话/只发触发词)时
        // 注入占位提问走正常对话,而不是静默丢弃(修复「@ 了却不回复」)
        let mut user_text = {
            let cfg = self.cfg.read().await;
            trigger::strip_keyword(text, &cfg.napcat.keyword).to_string()
        };
        if user_text.is_empty() {
            user_text = if msg.at_me || msg.reply_me {
                "(对方 @/引用了你,但没有附带文本内容)".to_string()
            } else {
                "(对方发出了触发词,但没有附带文本内容)".to_string()
            };
            self.log("debug", &format!("[{key}] 触发但无正文,使用占位提问"));
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
        let user_tokens = session.push_id("user", &user_text, ratio, &user_id, msg.user_id) as f64;
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
        self.rt.abort_insert(key, abort_tx);

        let (mut stream_result, pending_sent) = self
            .run_streamed_chat(key, turn, msg, &model, &msgs, reserve, abort_rx, pending_cfg, started)
            .await;

        // 正文为空(思考吃光预算/被截断)时关闭思考重试一次,仍失败才走错误分支
        if let Err(e) = &stream_result {
            if crate::llm::is_empty_reply_err(e) {
                self.log("warn", &format!("[{key}] {e},关闭思考重试一次"));
                let mut m2 = model.clone();
                m2.thinking = "disabled".into();
                stream_result = self.llm.chat(&m2, &msgs, Some(reserve)).await;
            }
        }

        self.rt.abort_remove(key);
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
                // 记忆标记:总是剥离(防止关闭状态泄漏到群里);仅开启时执行。
                // 剥离后统一清洗(空行折叠/去零宽字符),发送与落历史用同一份文本
                let mut out = reply.text;
                let (clean, ops) = memory::parse_memory_ops(&out);
                out = crate::outbound::sanitize_reply(&clean);
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
                let assistant_tokens = session.push_id("assistant", &out, ratio, &assistant_id, 0) as f64;
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

    /// 忙碌时入队(上限见 runtime::PENDING_CAP,超出丢最旧并记日志,防无限堆积)
    fn queue_pending(&self, key: &str, msg: ParsedMsg, turn: String) {
        if let Some((dropped, _)) = self.rt.pending_push(key, (msg, turn)) {
            let preview: String = dropped.text.chars().take(50).collect();
            self.log(
                "warn",
                &format!("[{key}] 排队已满,丢弃最旧消息: {preview}"),
            );
        }
    }

    /// 逐条补处理排队消息(full_dialogue 结束处调用;嵌套调用自然处理新增排队)
    async fn drain_queue(&self, key: &str) {
        loop {
            let Some((msg, turn)) = self.rt.pending_pop(key) else { return; };
            let sess = self.get_session(key).await;
            if sess.try_lock().is_err() {
                // 理论不应发生(锁已释放);放回队首,留待下次
                self.rt.pending_push_front(key, (msg, turn));
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
                            self.rt.live_remove(key);
                            return (Err(e), pending_sent);
                        }
                        Some(Ok(StreamEvent::Reasoning { delta })) => {
                            if first_token_at.is_none() {
                                first_token_at = Some(Instant::now());
                            }
                            self.set_status(key, SessionStatus::Thinking).await;
                            // 直播缓冲(前端轮询兜底)
                            self.rt.live_append(key, turn, true, &delta);
                        }
                        Some(Ok(StreamEvent::Content { delta })) => {
                            if first_token_at.is_none() {
                                first_token_at = Some(Instant::now());
                            }
                            got_content = true;
                            self.set_status(key, SessionStatus::Replying).await;
                            self.rt.live_append(key, turn, false, &delta);
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
        self.rt.live_remove(key);
        (stream.finish(), pending_sent)
    }


    /// 轻量通道:单轮插话(极小上下文、不落盘、不新开会话、失败静默)。
    /// 插话是单轮小请求,关闭思考模式(推理链纯属浪费且会吃光小预算)。
    async fn light_reply(&self, msg: &ParsedMsg, user_text: &str, turn: &str) {
        let (prompt, model, max_tokens) = {
            let cfg = self.cfg.read().await;
            (
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.active_model().cloned(),
                cfg.chat.interject.interject_max_tokens.max(16),
            )
        };
        let Some(mut model) = model else {
            return;
        };
        model.thinking = "disabled".into();
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
                content: format!("{}{}", speaker_prefix(msg.user_id), user_text),
            },
        ];
        let started = Instant::now();
        match self.llm.chat(&model, &msgs, Some(max_tokens)).await {
            Ok(reply) => {
                self.record_usage(&model, "interject", &reply.usage);
                self.emit_llm_stats(&model, &reply.usage, started.elapsed().as_millis() as u64).await;
                // 插话场景剥离记忆标记但不执行(轻量通道不管理记忆);清洗后为空则静默
                // (插话没话说就闭嘴,比发占位符自然)
                let (text, _) = memory::parse_memory_ops(&reply.text);
                let out = crate::outbound::sanitize_reply(&text);
                if out.is_empty() {
                    self.log("debug", &format!("[{key}] 插话无内容,跳过发送"));
                    return;
                }
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


    // ---------- 上下文预算 ----------



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

    /// 发送回复:清洗后的文本 → CQ 转义(防注入)→ 段落边界分段(超长单段才硬切)
    pub(crate) async fn send_text(&self, msg: &ParsedMsg, text: &str) -> Result<(), String> {
        let (max_len, delay) = {
            let cfg = self.cfg.read().await;
            (cfg.napcat.max_msg_len.max(1) as usize, cfg.napcat.segment_delay_ms)
        };
        let chunks = crate::outbound::segment_text(text, max_len);
        if chunks.is_empty() {
            return Ok(()); // 清洗后为空的极端情况:不发空消息
        }
        if chunks.len() > 1 {
            self.log(
                "info",
                &format!("[{}] 回复较长,分 {} 段发送", session_key(msg), chunks.len()),
            );
        }
        for (i, chunk) in chunks.iter().enumerate() {
            let escaped = crate::outbound::cq_escape(chunk);
            let r = match msg.kind {
                MsgKind::Group => {
                    self.sender
                        .send_group_msg(msg.group_id.unwrap_or(0), &escaped)
                        .await
                }
                MsgKind::Private => self.sender.send_private_msg(msg.user_id, &escaped).await,
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

    pub(crate) async fn emit_session(&self, key: &str, session: &Session) {
        self.events.lock().unwrap().push(FrontendEvent::SessionChanged {
            key: key.to_string(),
            count: session.history.len(),
            tokens: session.total_tokens(),
        });
    }

    // ---------- 活跃度与插话 ----------


    // ---------- 对外接口(命令层 / GUI) ----------

    /// 会话清理循环:空闲超时移除内存(文件保留)
    pub async fn cleaner_loop(self: Arc<Self>, stop: tokio_util::sync::CancellationToken) {
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    // 运行时状态统一清理(过期轨迹/空活跃窗口/状态归位/残留直播与排队)
                    self.rt.cleanup();
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
                    // 串行门随会话一并清理
                    self.rt.drop_gates_except(&map).await;
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
            // 会话级暂停(/pause):列表显示独立于运行状态灯
            item["paused"] = json!(self.rt.is_session_paused(&key));
        }
        list
    }

    /// 会话详情(轨迹时间线 + 摘要信息 + 进行中回复的直播缓冲);机器人未运行时由命令层直接读盘
    pub async fn session_detail(&self, key: &str) -> serde_json::Value {
        let trace_path = self.trace_dir.join(format!("{key}.jsonl"));
        let events = TraceStore::read_all(&trace_path);
        let file = self.sessions_dir.join(format!("{key}.jsonl"));
        let (count, tokens, has_summary, summary) = read_history_summary(&file);
        let live = self.rt.live_get(key);
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



/// 编辑一条历史消息(同时同步轨迹与内存会话)
pub async fn update_history_msg(&self, key: &str, id: &str, text: &str) -> Result<(), String> {
    let ratio = {
        let cfg = self.cfg.read().await;
        cfg.chat.estimate_ratio
    };
    let file = self.sessions_dir.join(format!("{key}.jsonl"));
    rewrite_history_entry(&file, id, text, ratio)?;
    TraceStore::rewrite_text_by_id(&self.trace_dir.join(format!("{key}.jsonl")), id, text);
    self.reload_session(key).await?;
    self.log("info", &format!("[{key}] 已改写历史消息 {id}"));
    Ok(())
}

/// 删除一条历史消息(同时删除轨迹中对应事件与内存会话)
pub async fn delete_history_msg(&self, key: &str, id: &str) -> Result<(), String> {
    let file = self.sessions_dir.join(format!("{key}.jsonl"));
    remove_history_entry(&file, id)?;
    TraceStore::remove_by_id(&self.trace_dir.join(format!("{key}.jsonl")), id);
    self.reload_session(key).await?;
    self.log("info", &format!("[{key}] 已删除历史消息 {id}"));
    Ok(())
}

    /// 停止进行中的回复(存在运行中的流式请求时生效)
    pub fn stop_session(&self, key: &str) -> bool {
        let hit = self.rt.abort_send(key);
        if hit {
            self.log("info", &format!("[{key}] 收到停止指令"));
        }
        hit
    }

    pub async fn clear_session(&self, key: &str) {
        let sess = self.get_session(key).await;
        let locked = sess.try_lock();
        if let Ok(mut s) = locked {
            s.ensure_loaded();
            s.clear();
            // 与 /clear 一致:群聊轨迹一并清空,避免清空后仍注入旧消息
            self.rt.trail_remove(key);
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









#[cfg(test)]
mod tests {

}
