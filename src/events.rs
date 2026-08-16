//! 前端事件总线(拉模式)与会话展示状态。
//!
//! 事件走 `get_events` 轮询拉取而非 Tauri 推送:推送链路在部分环境下不可用,
//! invoke 拉取已被证明可靠(设计初衷,保持不变)。

use serde::Serialize;
use std::collections::VecDeque;

use crate::napcat::ConnStatus;
use crate::placement;
use crate::trace::TraceEvent;

/// 记忆管理说明:恒定的独立 system 消息(与开关无关,开关切换不影响缓存前缀)。
/// ⚠️ 此文本内容改动会破坏缓存前缀,勿随意修改。
pub const MEMORY_GUIDE: &str = "(你可以管理长期记忆:当你了解到值得长期记住的信息(用户偏好、重要事实、约定)时,在回复末尾用标记 [记忆:添加 内容] 写入;需要删除时用 [记忆:删除 内容片段]。不要写入临时性信息,每次只写最重要的。同一事实发生变化时,必须先用 [记忆:删除] 移除旧条目再 [记忆:添加] 新条目,避免同时存在互相矛盾的记忆。)";

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
    /// 连接告警(QQ 离线 / 心跳丢失;raised=false 为恢复)——GUI 顶部横幅 + 日志
    Alarm { text: String, raised: bool },
    /// 记忆位置切换提案(醒目弹窗审批)
    PlacementProposal { proposal: placement::Proposal },
}

/// 事件环形缓冲(拉模式):前端通过 get_events 轮询拉取。
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
#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    #[default]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_labels() {
        assert_eq!(SessionStatus::Idle.as_str(), "idle");
        assert_eq!(SessionStatus::Thinking.label(), "思考中");
    }

    #[test]
    fn event_buf_seq() {
        let mut buf = EventBuf::new();
        buf.push(FrontendEvent::Log { level: "info".into(), msg: "a".into() });
        buf.push(FrontendEvent::Log { level: "info".into(), msg: "b".into() });
        let (evs, latest) = buf.after(0);
        assert_eq!(evs.len(), 2);
        assert_eq!(latest, 2);
        let (evs2, _) = buf.after(1);
        assert_eq!(evs2.len(), 1);
    }
}
