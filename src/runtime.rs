//! 会话运行时状态注册表。
//!
//! 此前活跃度/消息计数/插话冷却/轨迹/状态灯/中止通道/排队消息/直播缓冲是
//! 8 张以 session key 为键的平行 HashMap,各自加锁、各自清理,天然容易不一致。
//! 现在合并为单一 `SessionRegistry`:每会话一个 `SessionState`,单锁短临界区,
//! 清理一次遍历原子完成。
//!
//! 锁纪律:所有方法内部完成加锁-修改-释放,绝不跨 await 持锁
//! (串行门 gate 除外 —— 它本身就是用来跨 await 持有的 tokio Mutex)。

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::{watch, Mutex, RwLock};

use crate::events::{LiveTurn, SessionStatus};
use crate::napcat::ParsedMsg;

/// 轨迹行:(时间, 发送者 QQ, 文本)
pub type TrailLine = (SystemTime, i64, String);

/// 排队消息:(消息, turn id)
pub type PendingMsg = (ParsedMsg, String);

/// 单个会话的全部运行时状态
#[derive(Default)]
pub struct SessionState {
    /// 会话状态(列表胶囊灯)
    pub status: SessionStatus,
    /// 会话级暂停(/pause 设置):仅接收消息,不决策不回复(重启后复位,与全局暂停一致)
    pub paused: bool,
    /// 进行中回复的直播缓冲(前端轮询兜底)
    pub live: Option<LiveTurn>,
    /// 进行中回复的中止通道
    pub abort: Option<watch::Sender<bool>>,
    /// 会话忙碌时的暂存队列(回合结束后按序补处理)
    pub pending: VecDeque<PendingMsg>,
    /// 累计消息计数(插话冷却用;跨会话清理保留)
    pub msg_count: u64,
    /// 最近一次主动发言时的消息计数(插话冷却)
    pub interject_at: Option<u64>,
    /// 最近窗口内的消息时间戳(活跃度)
    pub activity: VecDeque<Instant>,
    /// 群聊轨迹(未触发对话的普通消息)
    pub trail: VecDeque<TrailLine>,
}

pub struct SessionRegistry {
    states: StdMutex<HashMap<String, SessionState>>,
    /// 每会话消息串行门(跨会话并行:同群保序,不同群/私聊互不阻塞)
    gates: RwLock<HashMap<String, Arc<Mutex<()>>>>,
}

use std::sync::Arc;

/// 排队队列上限(超出丢最旧,防无限堆积)
pub const PENDING_CAP: usize = 8;

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            states: StdMutex::new(HashMap::new()),
            gates: RwLock::new(HashMap::new()),
        }
    }

    fn with<R>(&self, key: &str, f: impl FnOnce(&mut SessionState) -> R) -> R {
        let mut m = self.states.lock().unwrap();
        f(m.entry(key.to_string()).or_default())
    }

    // ---------- 状态灯 ----------

    /// 设置会话状态;状态实际变化时返回新值(供调用方发事件),未变返回 None
    pub fn set_status(&self, key: &str, s: SessionStatus) -> Option<SessionStatus> {
        self.with(key, |st| {
            if st.status == s {
                None
            } else {
                st.status = s;
                Some(s)
            }
        })
    }

    pub fn get_status(&self, key: &str) -> SessionStatus {
        self.states
            .lock()
            .unwrap()
            .get(key)
            .map(|s| s.status)
            .unwrap_or(SessionStatus::Idle)
    }

    // ---------- 会话级暂停 ----------

    /// 设置会话暂停(/pause、/resume);返回是否发生了变化
    pub fn set_session_paused(&self, key: &str, paused: bool) -> bool {
        self.with(key, |st| {
            let changed = st.paused != paused;
            st.paused = paused;
            changed
        })
    }

    pub fn is_session_paused(&self, key: &str) -> bool {
        self.states
            .lock()
            .unwrap()
            .get(key)
            .map(|s| s.paused)
            .unwrap_or(false)
    }

    // ---------- 直播缓冲 ----------

    /// 追加流式增量(thinking=true 为思考增量,否则正文)
    pub fn live_append(&self, key: &str, turn: &str, thinking: bool, delta: &str) {
        self.with(key, |st| {
            let lt = st.live.get_or_insert_with(|| LiveTurn {
                turn: turn.to_string(),
                reasoning: String::new(),
                content: String::new(),
            });
            if thinking {
                lt.reasoning.push_str(delta);
            } else {
                lt.content.push_str(delta);
            }
        });
    }

    pub fn live_remove(&self, key: &str) {
        self.with(key, |st| st.live = None);
    }

    pub fn live_get(&self, key: &str) -> Option<LiveTurn> {
        self.states.lock().unwrap().get(key).and_then(|s| s.live.clone())
    }

    // ---------- 中止通道 ----------

    pub fn abort_insert(&self, key: &str, tx: watch::Sender<bool>) {
        self.with(key, |st| st.abort = Some(tx));
    }

    pub fn abort_remove(&self, key: &str) {
        self.with(key, |st| st.abort = None);
    }

    /// 中止指定会话进行中的回复;返回是否存在进行中的请求
    pub fn abort_send(&self, key: &str) -> bool {
        let tx = {
            let m = self.states.lock().unwrap();
            m.get(key).and_then(|s| s.abort.clone())
        };
        match tx {
            Some(tx) => {
                let _ = tx.send(true);
                true
            }
            None => false,
        }
    }

    /// 中止所有会话进行中的回复(全局暂停)
    pub fn abort_send_all(&self) {
        let txs: Vec<watch::Sender<bool>> = {
            let m = self.states.lock().unwrap();
            m.values().filter_map(|s| s.abort.clone()).collect()
        };
        for tx in txs {
            let _ = tx.send(true);
        }
    }

    // ---------- 排队 ----------

    /// 入队(超上限丢最旧,返回被丢弃的消息供日志)
    pub fn pending_push(&self, key: &str, item: PendingMsg) -> Option<PendingMsg> {
        self.with(key, |st| {
            let dropped = if st.pending.len() >= PENDING_CAP {
                st.pending.pop_front()
            } else {
                None
            };
            st.pending.push_back(item);
            dropped
        })
    }

    pub fn pending_pop(&self, key: &str) -> Option<PendingMsg> {
        self.with(key, |st| st.pending.pop_front())
    }

    /// 放回队首(锁竞争异常时留待下次)
    pub fn pending_push_front(&self, key: &str, item: PendingMsg) {
        self.with(key, |st| st.pending.push_front(item));
    }

    pub fn pending_clear(&self) {
        let mut m = self.states.lock().unwrap();
        for st in m.values_mut() {
            st.pending.clear();
        }
    }

    // ---------- 活跃度与插话冷却 ----------

    /// 记录一条消息:滑动活跃窗口推进 + 累计计数递增
    pub fn track_activity(&self, key: &str, window_secs: u64) {
        self.with(key, |st| {
            let now = Instant::now();
            let cutoff = now - Duration::from_secs(window_secs.max(1));
            while st.activity.front().map(|t| *t < cutoff).unwrap_or(false) {
                st.activity.pop_front();
            }
            st.activity.push_back(now);
            st.msg_count += 1;
        });
    }

    /// 消息速率(条/分钟)
    pub fn activity_rate(&self, key: &str, window_minutes: u64) -> f64 {
        let m = self.states.lock().unwrap();
        m.get(key)
            .map(|s| s.activity.len() as f64 / window_minutes.max(1) as f64)
            .unwrap_or(0.0)
    }

    /// 距上次主动发言以来的新消息数(None = 从未主动发言)
    pub fn interject_since(&self, key: &str) -> Option<u64> {
        let m = self.states.lock().unwrap();
        m.get(key)
            .and_then(|s| s.interject_at.map(|last| s.msg_count.saturating_sub(last)))
    }

    /// 刷新插话冷却(记录当前计数为最近一次主动发言)
    pub fn mark_interjected(&self, key: &str) {
        self.with(key, |st| st.interject_at = Some(st.msg_count));
    }

    // ---------- 群聊轨迹 ----------

    /// 追加轨迹并按窗口与条数收紧(max_entries == 0 表示不限条数)
    pub fn trail_push(&self, key: &str, text: &str, sender: i64, window_secs: u64, max_entries: usize) {
        let text = text.trim();
        if text.is_empty() {
            return;
        }
        self.with(key, |st| {
            let now = SystemTime::now();
            st.trail.retain(|(t, _, _)| {
                now.duration_since(*t)
                    .map(|d| d.as_secs() < window_secs)
                    .unwrap_or(false)
            });
            st.trail.push_back((now, sender, text.to_string()));
            if max_entries > 0 {
                while st.trail.len() > max_entries {
                    st.trail.pop_front();
                }
            }
        });
    }

    pub fn trail_get(&self, key: &str) -> VecDeque<TrailLine> {
        self.states
            .lock()
            .unwrap()
            .get(key)
            .map(|s| s.trail.clone())
            .unwrap_or_default()
    }

    pub fn trail_remove(&self, key: &str) {
        self.with(key, |st| st.trail.clear());
    }

    // ---------- 串行门 ----------

    /// 每会话串行门(跨会话并行:同群保序,不同群互不阻塞)
    pub async fn gate(&self, key: &str) -> Arc<Mutex<()>> {
        {
            let map = self.gates.read().await;
            if let Some(g) = map.get(key) {
                return g.clone();
            }
        }
        let mut map = self.gates.write().await;
        map.entry(key.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    // ---------- 清理 ----------

    /// 周期清理:过期轨迹、空活跃窗口、非空闲状态灯。
    /// msg_count / interject_at 为累计计数,跨清理保留(与旧行为一致);
    /// live / abort / pending 仅在会话空闲时视为残留并清除
    /// (进行中的回合依赖它们:清了停止按钮就失效、排队消息就丢了)。
    pub fn cleanup(&self) {
        let now = SystemTime::now();
        let mut m = self.states.lock().unwrap();
        for st in m.values_mut() {
            st.trail.retain(|(t, _, _)| {
                now.duration_since(*t)
                    .map(|d| d.as_secs() < 86400)
                    .unwrap_or(false)
            });
            if st.status != SessionStatus::Idle {
                st.status = SessionStatus::Idle;
                // 非空闲说明有进行中回合:跳过残留清理,留给回合结束时的自我清理
                continue;
            }
            st.live = None;
            st.abort = None;
            st.pending.clear();
        }
        // 活跃窗口为空且没有任何留存价值的会话状态整个移除
        // (保留有累计计数、轨迹或暂停标志的,防止插话冷却被意外重置、暂停状态被清理丢掉)
        m.retain(|_, st| {
            !st.activity.is_empty()
                || st.msg_count > 0
                || !st.trail.is_empty()
                || st.paused
                || st.status != SessionStatus::Idle
                || st.live.is_some()
                || st.abort.is_some()
                || !st.pending.is_empty()
        });
    }

    /// 串行门随会话移除一并清理(仅保留仍在 sessions 表中的会话的门)
    pub async fn drop_gates_except(&self, keep: &HashMap<String, Arc<Mutex<crate::session::Session>>>) {
        self.gates.write().await.retain(|k, _| keep.contains_key(k));
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_change_events_only_on_transition() {
        let rt = SessionRegistry::new();
        assert_eq!(rt.get_status("g1"), SessionStatus::Idle);
        assert_eq!(rt.set_status("g1", SessionStatus::Replying), Some(SessionStatus::Replying));
        // 相同状态不重复上报
        assert_eq!(rt.set_status("g1", SessionStatus::Replying), None);
        assert_eq!(rt.get_status("g1"), SessionStatus::Replying);
    }

    #[test]
    fn live_append_and_remove() {
        let rt = SessionRegistry::new();
        rt.live_append("g1", "t1", true, "思");
        rt.live_append("g1", "t1", true, "考");
        rt.live_append("g1", "t1", false, "答");
        let lt = rt.live_get("g1").unwrap();
        assert_eq!(lt.reasoning, "思考");
        assert_eq!(lt.content, "答");
        rt.live_remove("g1");
        assert!(rt.live_get("g1").is_none());
    }

    #[test]
    fn pending_cap_and_order() {
        let rt = SessionRegistry::new();
        let mk = |i: usize| {
            (
                ParsedMsg {
                    kind: crate::napcat::MsgKind::Group,
                    group_id: Some(1),
                    user_id: i as i64,
                    text: format!("m{i}"),
                    at_me: false,
                    reply_me: false,
                    is_self: false,
                    raw: serde_json::Value::Null,
                },
                format!("t{i}"),
            )
        };
        for i in 0..10 {
            let dropped = rt.pending_push("g1", mk(i));
            if i >= PENDING_CAP {
                // 超上限后每次入队丢最旧
                let d = dropped.expect("应丢弃最旧");
                assert_eq!(d.0.text, format!("m{}", i - PENDING_CAP));
            } else {
                assert!(dropped.is_none());
            }
        }
        assert_eq!(rt.pending_pop("g1").unwrap().0.text, "m2");
        let item = rt.pending_pop("g1").unwrap();
        rt.pending_push_front("g1", item);
        assert_eq!(rt.pending_pop("g1").unwrap().0.text, "m3");
    }

    #[test]
    fn interject_cooldown_counts() {
        let rt = SessionRegistry::new();
        assert_eq!(rt.interject_since("g1"), None);
        for _ in 0..5 {
            rt.track_activity("g1", 60);
        }
        rt.mark_interjected("g1");
        assert_eq!(rt.interject_since("g1"), Some(0));
        rt.track_activity("g1", 60);
        assert_eq!(rt.interject_since("g1"), Some(1));
    }

    #[test]
    fn session_pause_survives_cleanup() {
        let rt = SessionRegistry::new();
        assert!(!rt.is_session_paused("g1"));
        assert!(rt.set_session_paused("g1", true));   // 变化返回 true
        assert!(!rt.set_session_paused("g1", true));  // 重复设置无变化
        assert!(rt.is_session_paused("g1"));
        // 周期清理不得清除暂停标志(用户显式设置的状态)
        rt.cleanup();
        assert!(rt.is_session_paused("g1"));
        assert!(rt.set_session_paused("g1", false));
        assert!(!rt.is_session_paused("g1"));
        // 解除后且无其他留存价值,条目可被清理回收
        rt.cleanup();
        assert!(!rt.is_session_paused("g1"));
    }

    #[test]
    fn trail_window_and_cap() {
        let rt = SessionRegistry::new();
        rt.trail_push("g1", "a", 1, 3600, 2);
        rt.trail_push("g1", "b", 2, 3600, 2);
        rt.trail_push("g1", "c", 3, 3600, 2);
        let t = rt.trail_get("g1");
        assert_eq!(t.len(), 2);
        assert_eq!(t[0].2, "b"); // 最旧被挤出
        rt.trail_remove("g1");
        assert!(rt.trail_get("g1").is_empty());
    }
}
