//! 会话存储层:历史消息、摘要、持久化(JSONL)与磁盘级会话操作。
//!
//! 文件格式:`sessions/{key}.jsonl`,首行 `role == "__summary__"` 为折叠摘要,
//! 其后为历史消息;重启后同一会话继续追加,缓存可跨重启延续(设计初衷)。
//! 磁盘是唯一真相:内存中的 Session 只是文件的懒加载视图,截断/折叠后 rewrite 全量重写。

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::estimate::estimate_tokens;
use crate::memory::MemoryStore;
use crate::napcat::{MsgKind, ParsedMsg};
use crate::trace;

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
    /// 发送者 QQ(0 = 未知/机器人自身;注入上下文时加 [QQxxx] 前缀,让模型分清说话人)
    #[serde(default)]
    pub sender: i64,
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
    pub fn new(key: &str, dir: &std::path::Path) -> Self {
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
    pub fn ensure_loaded(&mut self) {
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
    pub fn rewrite(&self) {
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
                    sender: 0,
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

    /// 追加一条并落盘,返回该条 token 估算。
    /// sender = 发送者 QQ(user 消息);assistant 与摘要传 0。
    pub fn push_id(&mut self, role: &str, text: &str, ratio: f64, id: &str, sender: i64) -> u32 {
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
            sender,
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
    pub(crate) fn push(&mut self, role: &str, text: &str, ratio: f64) {
        let id = trace::new_id();
        self.push_id(role, text, ratio, &id, 0);
    }

    pub fn clear(&mut self) {
        self.history.clear();
        self.summary = None;
        self.summary_tokens = 0;
        let _ = std::fs::remove_file(&self.file);
    }

    pub fn total_tokens(&self) -> u64 {
        self.history.iter().map(|h| h.tokens as u64).sum::<u64>()
            + self.summary_tokens as u64
    }
}

/// 会话键:群 `g{group_id}` / 私聊 `u{user_id}`
pub fn session_key(msg: &ParsedMsg) -> String {
    match msg.kind {
        MsgKind::Group => format!("g{}", msg.group_id.unwrap_or(0)),
        MsgKind::Private => format!("u{}", msg.user_id),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::estimate::estimate_tokens;

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
    fn history_entry_edit_delete() {
        let dir = std::env::temp_dir().join("lightbot_test_edit");
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::create_dir_all(&dir);
        let file = dir.join("g1.jsonl");
        let mut s = Session::new("g1", &dir);
        s.ensure_loaded();
        s.push_id("user", "你好", 1.0, "id1", 123);
        s.push_id("assistant", "你好呀", 1.0, "id2", 0);
        drop(s);

        // 改写
        rewrite_history_entry(&file, "id1", "改写后的内容", 1.0).unwrap();
        let (count, _, _, _) = read_history_summary(&file);
        assert_eq!(count, 2);
        // 找不到返回 Err
        assert!(rewrite_history_entry(&file, "不存在", "x", 1.0).is_err());

        // 删除
        remove_history_entry(&file, "id1").unwrap();
        let (count, _, _, _) = read_history_summary(&file);
        assert_eq!(count, 1);
        assert!(remove_history_entry(&file, "id1").is_err());

        // 重载后内容一致
        let mut s2 = Session::new("g1", &dir);
        s2.ensure_loaded();
        assert_eq!(s2.history.len(), 1);
        assert_eq!(s2.history[0].text, "你好呀");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
