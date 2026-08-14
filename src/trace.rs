//! 会话轨迹(审计流水):按会话记录「收到什么 / 决策器结论 / 思考了什么 / 模型输出 / 最终回复」。
//!
//! - 落盘:`sessions/traces/{key}.jsonl`,会话详情页的数据源;
//! - 与历史文件联动:对话消息共用同一个 `id`(MsgIn/MsgOut),详情页编辑/删除时
//!   同时改写历史文件与轨迹文件;
//! - 单会话轨迹最多保留 TRACE_CAP 条(超出丢最旧),防止长期会话文件无限膨胀。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::llm::Usage;

pub const TRACE_CAP: usize = 600;

/// 全局递增 + 时间戳的短 id(与历史条目共用)
pub fn new_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    format!("{ns:x}{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

pub fn now_ts() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 单条轨迹事件(serde tag 为 type)
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TraceEvent {
    /// 收到的 QQ 消息(id 与历史条目对应;ignored = 被忽略/命令等未进历史)
    MsgIn {
        id: Option<String>,
        turn: String,
        ts: i64,
        trigger: String,
        text: String,
        ignored: bool,
    },
    /// 斜杠命令及其回复(不进历史)
    Cmd {
        turn: String,
        ts: i64,
        text: String,
        reply: String,
    },
    /// 回复决策器的结论
    Decide {
        turn: String,
        ts: i64,
        text: String,
        verdict: bool,
        model: String,
        ms: u64,
    },
    /// 完整思考过程(回复结束后写入全量;直播增量走 TurnDelta 事件)
    Think {
        turn: String,
        ts: i64,
        text: String,
        tokens: u64,
    },
    /// 模型最终输出(即发送到 QQ 的回复;id 与历史条目对应)
    MsgOut {
        id: Option<String>,
        turn: String,
        ts: i64,
        text: String,
        model: String,
        usage: Usage,
    },
    /// 轻量插话输出(不落历史)
    LiteOut {
        turn: String,
        ts: i64,
        text: String,
        model: String,
    },
    /// 摘要折叠
    Fold {
        turn: String,
        ts: i64,
        folded: usize,
        summary_tokens: u32,
    },
    /// 错误 / 停止
    Error {
        turn: String,
        ts: i64,
        text: String,
    },
}

/// 追加 + 限量落盘的轨迹存储
pub struct TraceStore {
    file: PathBuf,
}

impl TraceStore {
    pub fn new(file: PathBuf) -> Self {
        Self { file }
    }

    /// 从磁盘读取全部事件(顺序即时间线)
    pub fn read_all(file: &Path) -> Vec<TraceEvent> {
        let mut out = Vec::new();
        if let Ok(content) = std::fs::read_to_string(file) {
            for line in content.lines() {
                if let Ok(ev) = serde_json::from_str::<TraceEvent>(line) {
                    out.push(ev);
                }
            }
        }
        out
    }

    /// 追加一条;超过上限时重写为最新 TRACE_CAP 条
    pub fn push(&self, ev: &TraceEvent) {
        let mut all = Self::read_all(&self.file);
        all.push(ev.clone());
        if all.len() > TRACE_CAP {
            all.drain(..all.len() - TRACE_CAP);
        }
        self.write(&all);
    }

    fn write(&self, events: &[TraceEvent]) {
        if let Some(dir) = self.file.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let mut out = String::new();
        for e in events {
            if let Ok(l) = serde_json::to_string(e) {
                out.push_str(&l);
                out.push('\n');
            }
        }
        let _ = std::fs::write(&self.file, out);
    }

    /// 改写所有携带该 id 的事件文本(编辑聊天记录时同步轨迹)
    pub fn rewrite_text_by_id(file: &Path, id: &str, text: &str) {
        let events = Self::read_all(file);
        let mut changed = false;
        let events: Vec<TraceEvent> = events
            .into_iter()
            .map(|mut e| {
                let target = match &mut e {
                    TraceEvent::MsgIn { id: tid, .. } | TraceEvent::MsgOut { id: tid, .. } => {
                        tid.as_deref() == Some(id)
                    }
                    _ => false,
                };
                if target {
                    changed = true;
                    match &mut e {
                        TraceEvent::MsgIn { text: t, .. } => *t = text.to_string(),
                        TraceEvent::MsgOut { text: t, .. } => *t = text.to_string(),
                        _ => {}
                    }
                }
                e
            })
            .collect();
        if changed {
            let store = TraceStore::new(file.to_path_buf());
            store.write(&events);
        }
    }

    /// 删除所有携带该 id 的事件,返回删除条数
    pub fn remove_by_id(file: &Path, id: &str) -> usize {
        let events = Self::read_all(file);
        let before = events.len();
        let kept: Vec<TraceEvent> = events
            .into_iter()
            .filter(|e| match e {
                TraceEvent::MsgIn { id: tid, .. } | TraceEvent::MsgOut { id: tid, .. } => {
                    tid.as_deref() != Some(id)
                }
                _ => true,
            })
            .collect();
        let removed = before - kept.len();
        if removed > 0 {
            let store = TraceStore::new(file.to_path_buf());
            store.write(&kept);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(file_name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("lightbot_trace_test");
        let _ = std::fs::create_dir_all(&dir);
        dir.join(file_name)
    }

    #[test]
    fn push_read_cap() {
        let p = tmp("g1.jsonl");
        let _ = std::fs::remove_file(&p);
        let s = TraceStore::new(p.clone());
        for i in 0..(TRACE_CAP + 20) {
            s.push(&TraceEvent::Cmd {
                turn: format!("t{i}"),
                ts: i as i64,
                text: "/stats".into(),
                reply: "ok".into(),
            });
        }
        let all = TraceStore::read_all(&p);
        assert_eq!(all.len(), TRACE_CAP);
        match &all[0] {
            TraceEvent::Cmd { turn, .. } => assert_eq!(turn, "t20"),
            _ => panic!("wrong type"),
        }
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn rewrite_and_remove_by_id() {
        let p = tmp("g2.jsonl");
        let _ = std::fs::remove_file(&p);
        let s = TraceStore::new(p.clone());
        s.push(&TraceEvent::MsgIn {
            id: Some("a1".into()),
            turn: "t1".into(),
            ts: 1,
            trigger: "at".into(),
            text: "你好".into(),
            ignored: false,
        });
        s.push(&TraceEvent::MsgOut {
            id: Some("a2".into()),
            turn: "t1".into(),
            ts: 2,
            text: "你好呀".into(),
            model: "m".into(),
            usage: Usage::default(),
        });
        s.push(&TraceEvent::Decide {
            turn: "t1".into(),
            ts: 0,
            text: "你好".into(),
            verdict: true,
            model: "m".into(),
            ms: 10,
        });
        TraceStore::rewrite_text_by_id(&p, "a1", "改过了");
        let all = TraceStore::read_all(&p);
        match &all[0] {
            TraceEvent::MsgIn { text, .. } => assert_eq!(text, "改过了"),
            _ => panic!(),
        }
        assert_eq!(TraceStore::remove_by_id(&p, "a2"), 1);
        assert_eq!(TraceStore::read_all(&p).len(), 2);
        let _ = std::fs::remove_file(&p);
    }
}
