//! 长期记忆系统。
//!
//! 缓存友好设计(针对 DeepSeek context caching):
//! - 记忆作为独立 system 消息,固定位于人设之后 → 记忆变化不影响人设前缀的缓存命中;
//! - 记忆采用「追加到末尾」更新:新记忆加在条目列表尾部,旧条目逐字节不变,
//!   前缀(旧记忆)继续命中;删除时整段重写,属低频事件,可接受;
//! - 记忆有总 token 上限(默认 1200),超出时删最旧条目。
//!
//! 文件是唯一真相:聊天时每次 refresh() 从文件读取(文件小,毫秒级),
//! 模型/用户/GUI 的修改都落盘,天然一致。

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::chat::estimate_tokens;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MemoryEntry {
    pub ts: i64,
    /// 来源: "user" 用户写入 / "model" 模型自动写入
    pub source: String,
    pub text: String,
}

pub struct MemoryStore {
    file: PathBuf,
    pub entries: Vec<MemoryEntry>,
}

impl MemoryStore {
    pub fn new(file: PathBuf) -> Self {
        Self {
            file,
            entries: Vec::new(),
        }
    }

    /// 从文件重新读取(文件是唯一真相)
    pub fn refresh(&mut self) {
        self.entries.clear();
        if let Ok(content) = std::fs::read_to_string(&self.file) {
            for line in content.lines() {
                if let Ok(e) = serde_json::from_str::<MemoryEntry>(line) {
                    self.entries.push(e);
                }
            }
        }
    }

    fn save(&self) {
        if let Some(dir) = self.file.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let mut out = String::new();
        for e in &self.entries {
            if let Ok(l) = serde_json::to_string(e) {
                out.push_str(&l);
                out.push('\n');
            }
        }
        let _ = std::fs::write(&self.file, out);
    }

    /// 添加一条记忆(去重、限长、限条数,追加到末尾),返回是否新增
    pub fn add(
        &mut self,
        text: &str,
        source: &str,
        max_entries: usize,
        max_chars: usize,
    ) -> bool {
        let text = text.trim();
        if text.is_empty() {
            return false;
        }
        if self.entries.iter().any(|e| e.text == text) {
            return false; // 去重
        }
        let text: String = text.chars().take(max_chars.max(1)).collect();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.entries.push(MemoryEntry {
            ts,
            source: source.to_string(),
            text,
        });
        // 超条数删最旧
        while self.entries.len() > max_entries.max(1) {
            self.entries.remove(0);
        }
        self.save();
        true
    }

    /// 按内容包含匹配删除,返回删除条数
    pub fn remove_contains(&mut self, needle: &str) -> usize {
        let needle = needle.trim();
        if needle.is_empty() {
            return 0;
        }
        let before = self.entries.len();
        self.entries.retain(|e| !e.text.contains(needle));
        let removed = before - self.entries.len();
        if removed > 0 {
            self.save();
        }
        removed
    }

    /// 按序号删除(1-based),返回是否删除
    pub fn remove_index(&mut self, idx: usize) -> bool {
        if idx == 0 || idx > self.entries.len() {
            return false;
        }
        self.entries.remove(idx - 1);
        self.save();
        true
    }

    /// 渲染为 system 消息内容(格式固定,保证缓存前缀稳定)
    pub fn system_text(&self) -> String {
        let mut s = String::from("[长期记忆]\n");
        for e in &self.entries {
            s.push_str(&format!(
                "- ({} {}) {}\n",
                if e.source == "model" { "自动" } else { "用户" },
                fmt_ts(e.ts),
                e.text
            ));
        }
        s
    }

    pub fn total_tokens(&self, ratio: f64) -> u32 {
        let mut t = 0u32;
        for e in &self.entries {
            t = t.saturating_add(estimate_tokens(&e.text, ratio));
        }
        t
    }

    /// 总 token 超预算时删最旧条目
    pub fn trim_to_tokens(&mut self, budget: u32, ratio: f64) {
        while self.total_tokens(ratio) > budget && !self.entries.is_empty() {
            self.entries.remove(0);
        }
        self.save();
    }

    /// 供 GUI 的序列化视图
    pub fn to_values(&self) -> Vec<serde_json::Value> {
        self.entries
            .iter()
            .enumerate()
            .map(|(i, e)| {
                serde_json::json!({
                    "index": i + 1,
                    "ts": e.ts,
                    "date": fmt_ts(e.ts),
                    "source": e.source,
                    "text": e.text,
                })
            })
            .collect()
    }

    #[allow(dead_code)] // 供测试与外部工具使用
    pub fn file_path(&self) -> &std::path::Path {
        &self.file
    }
}

/// 时间显示 MM-DD
fn fmt_ts(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|d| d.format("%m-%d").to_string())
        .unwrap_or_default()
}

/// 模型回复中的记忆操作标记
#[derive(Debug, Clone, PartialEq)]
pub enum MemoryOp {
    Add(String),
    Remove(String),
}

/// 解析回复文本中的 [记忆:添加 xxx] / [记忆:删除 xxx] 标记。
/// 返回(剥离标记后的干净文本, 操作列表)。标记外的文本原样保留;
/// 未闭合的标记视为普通文本,不解析。
pub fn parse_memory_ops(text: &str) -> (String, Vec<MemoryOp>) {
    let mut clean = String::new();
    let mut ops = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("[记忆:") {
        match rest[start..].find(']') {
            Some(i) => {
                clean.push_str(&rest[..start]);
                let tag = &rest[start..start + i + 1];
                let inner = tag
                    .trim_start_matches("[记忆:")
                    .trim_end_matches(']')
                    .trim();
                if let Some(content) = inner.strip_prefix("添加") {
                    let c = content.trim();
                    if !c.is_empty() {
                        ops.push(MemoryOp::Add(c.to_string()));
                    }
                } else if let Some(content) = inner.strip_prefix("删除") {
                    let c = content.trim();
                    if !c.is_empty() {
                        ops.push(MemoryOp::Remove(c.to_string()));
                    }
                }
                rest = &rest[start + i + 1..];
            }
            None => {
                // 未闭合标记:原样保留,结束解析
                clean.push_str(rest);
                rest = "";
            }
        }
    }
    clean.push_str(rest);
    (clean, ops)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_store(name: &str) -> MemoryStore {
        let dir = std::env::temp_dir().join(format!("lightbot_mem_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        MemoryStore::new(dir.join("memories").join("g1.jsonl"))
    }

    #[test]
    fn add_dedupe_limits_roundtrip() {
        let mut m = tmp_store("a");
        assert!(m.add("用户叫阿伟", "user", 30, 200));
        assert!(!m.add("用户叫阿伟", "user", 30, 200)); // 去重
        assert!(m.add("用户喜欢简洁", "model", 30, 200));
        assert_eq!(m.entries.len(), 2);
        assert_eq!(m.entries[0].source, "user");
        assert_eq!(m.entries[1].source, "model");

        // 重新读取(文件是真相)
        let mut m2 = MemoryStore::new(m.file_path().to_path_buf());
        m2.refresh();
        assert_eq!(m2.entries.len(), 2);
        assert_eq!(m2.entries[1].text, "用户喜欢简洁");

        // 条数上限:第 3 条挤掉最旧
        assert!(m.add("第三条", "user", 2, 200));
        assert_eq!(m.entries.len(), 2);
        assert_eq!(m.entries[0].text, "用户喜欢简洁");

        // 单条长度上限
        assert!(m.add("很长很长的内容", "user", 30, 2));
        assert_eq!(m.entries.last().unwrap().text, "很长");

        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("lightbot_mem_a"));
    }

    #[test]
    fn remove_ops() {
        let mut m = tmp_store("b");
        m.add("A", "user", 30, 200);
        m.add("B", "user", 30, 200);
        m.add("C", "user", 30, 200);
        assert_eq!(m.remove_contains("B"), 1);
        assert_eq!(m.entries.len(), 2);
        assert_eq!(m.remove_contains("不存在"), 0);
        assert!(m.remove_index(2)); // 删 C(现在索引 2 是 C)
        assert_eq!(m.entries.len(), 1);
        assert!(!m.remove_index(5));
        assert!(!m.remove_index(0));

        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("lightbot_mem_b"));
    }

    #[test]
    fn trim_to_tokens() {
        let mut m = tmp_store("c");
        m.add("一二三四五六七八九十", "user", 100, 200); // ~13 tokens
        m.add("一二三四五六七八九十", "user", 100, 200); // 去重,只有 1 条
        m.add("不同内容", "user", 100, 200);
        m.trim_to_tokens(10, 1.0);
        assert_eq!(m.entries.len(), 1); // 第一条被删(最旧)

        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("lightbot_mem_c"));
    }

    #[test]
    fn parse_tags() {
        let (clean, ops) = parse_memory_ops("好的,我记住了。[记忆:添加 用户叫阿伟] 还有什么问题吗?");
        assert_eq!(clean, "好的,我记住了。 还有什么问题吗?");
        assert_eq!(ops, vec![MemoryOp::Add("用户叫阿伟".to_string())]);

        let (clean, ops) = parse_memory_ops("[记忆:删除 阿伟]正文");
        assert_eq!(clean, "正文");
        assert_eq!(ops, vec![MemoryOp::Remove("阿伟".to_string())]);

        // 无标记原样返回
        let (clean, ops) = parse_memory_ops("普通回复");
        assert_eq!(clean, "普通回复");
        assert!(ops.is_empty());

        // 空内容忽略
        let (_, ops) = parse_memory_ops("[记忆:添加 ]");
        assert!(ops.is_empty());

        // 多个标记
        let (_, ops) = parse_memory_ops("[记忆:添加 甲][记忆:删除 乙]");
        assert_eq!(ops.len(), 2);

        // 未闭合标记:保留原样
        let (clean, ops) = parse_memory_ops("你好[记忆:添加");
        assert_eq!(clean, "你好[记忆:添加");
        assert!(ops.is_empty());
    }
}
