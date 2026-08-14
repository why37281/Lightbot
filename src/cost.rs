//! 用量与费用追踪。
//!
//! 设计:
//! - 每次 LLM 调用按天追加落盘(`usage/YYYY-MM-DD.jsonl`),文件即真相;
//! - 每条记录保存「调用当时的价格快照」——之后用户在 GUI 改价,历史费用不会漂移;
//! - 内存只聚合「今天」的摘要,跨天自动切换日期文件;
//! - 全部写入是追加(append),磁盘开销极小。

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// 一次 LLM 调用的用量记录(落盘格式)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct UsageRecord {
    pub ts: i64,
    pub model: String,
    /// 调用类别:"dialogue" 完整对话 / "decide" 决策器 / "summarize" 摘要折叠 / "interject" 插话
    pub category: String,
    pub prompt: u64,
    pub cache_hit: u64,
    pub cache_miss: u64,
    pub completion: u64,
    pub reasoning: u64,
    /// 调用当时的价格快照(元 / 1M tokens)
    pub price_input: f64,
    pub price_cache_hit: f64,
    pub price_output: f64,
}

impl UsageRecord {
    /// 本次调用费用(元)
    pub fn cost(&self) -> f64 {
        (self.cache_hit as f64 * self.price_cache_hit
            + self.cache_miss as f64 * self.price_input
            + self.completion as f64 * self.price_output)
            / 1e6
    }
}

/// 单类别的聚合
#[derive(Serialize, Clone, Debug, Default)]
pub struct CategorySummary {
    pub calls: u64,
    pub prompt: u64,
    pub cache_hit: u64,
    pub cache_miss: u64,
    pub completion: u64,
    pub reasoning: u64,
    pub cost: f64,
}

/// 今日总聚合
#[derive(Serialize, Clone, Debug, Default)]
pub struct CostSummary {
    pub calls: u64,
    pub prompt: u64,
    pub cache_hit: u64,
    pub cache_miss: u64,
    pub completion: u64,
    pub reasoning: u64,
    pub cost: f64,
}

pub struct CostTracker {
    dir: PathBuf,
    /// 当前聚合的日期(YYYY-MM-DD)
    day: String,
    records: Vec<UsageRecord>,
}

impl CostTracker {
    pub fn new(dir: PathBuf) -> Self {
        let day = today();
        let mut t = Self {
            dir,
            day,
            records: Vec::new(),
        };
        t.load_today();
        t
    }

    fn file_for(&self, day: &str) -> PathBuf {
        self.dir.join(format!("{day}.jsonl"))
    }

    fn load_today(&mut self) {
        self.records.clear();
        if let Ok(content) = std::fs::read_to_string(self.file_for(&self.day)) {
            for line in content.lines() {
                if let Ok(r) = serde_json::from_str::<UsageRecord>(line) {
                    self.records.push(r);
                }
            }
        }
    }

    /// 跨天检测 + 记录一次调用
    pub fn record(&mut self, r: UsageRecord) {
        let day = today();
        if day != self.day {
            self.day = day;
            self.load_today();
        }
        let _ = std::fs::create_dir_all(&self.dir);
        let line = serde_json::to_string(&r).unwrap_or_default();
        use std::io::Write;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.file_for(&self.day))
        {
            let _ = writeln!(f, "{line}");
        }
        self.records.push(r);
    }

    fn fold(records: &[UsageRecord]) -> CostSummary {
        let mut s = CostSummary::default();
        for r in records {
            s.calls += 1;
            s.prompt += r.prompt;
            s.cache_hit += r.cache_hit;
            s.cache_miss += r.cache_miss;
            s.completion += r.completion;
            s.reasoning += r.reasoning;
            s.cost += r.cost();
        }
        s
    }

    /// 今日聚合(全部类别)
    pub fn today(&self) -> CostSummary {
        Self::fold(&self.records)
    }

    /// 今日按类别聚合(固定顺序:对话/决策/摘要/插话)
    pub fn by_category(&self) -> Vec<(String, CategorySummary)> {
        let mut map: Vec<(String, CategorySummary)> = vec![
            ("dialogue".into(), CategorySummary::default()),
            ("decide".into(), CategorySummary::default()),
            ("summarize".into(), CategorySummary::default()),
            ("interject".into(), CategorySummary::default()),
        ];
        for r in &self.records {
            let slot = map
                .iter_mut()
                .find(|(c, _)| c == &r.category)
                .map(|(_, s)| s);
            let Some(s) = slot else { continue };
            s.calls += 1;
            s.prompt += r.prompt;
            s.cache_hit += r.cache_hit;
            s.cache_miss += r.cache_miss;
            s.completion += r.completion;
            s.reasoning += r.reasoning;
            s.cost += r.cost();
        }
        map.retain(|(_, s)| s.calls > 0);
        map
    }
}

fn today() -> String {
    chrono::Local::now().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(category: &str, hit: u64, miss: u64) -> UsageRecord {
        UsageRecord {
            ts: 0,
            model: "deepseek-v4-flash".into(),
            category: category.into(),
            prompt: hit + miss,
            cache_hit: hit,
            cache_miss: miss,
            completion: 100,
            reasoning: 0,
            price_input: 1.0,
            price_cache_hit: 0.02,
            price_output: 2.0,
        }
    }

    #[test]
    fn cost_math() {
        // 命中 1M + 未命中 1M + 输出 1M = 0.02 + 1 + 2 = 3.02 元
        let r = UsageRecord {
            ts: 0,
            model: "m".into(),
            category: "dialogue".into(),
            prompt: 2_000_000,
            cache_hit: 1_000_000,
            cache_miss: 1_000_000,
            completion: 1_000_000,
            reasoning: 0,
            price_input: 1.0,
            price_cache_hit: 0.02,
            price_output: 2.0,
        };
        assert!((r.cost() - 3.02).abs() < 1e-9);
    }

    #[test]
    fn tracker_roundtrip() {
        let dir = std::env::temp_dir().join("lightbot_cost_test");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let mut t = CostTracker::new(dir.clone());
            t.record(rec("dialogue", 500, 100));
            t.record(rec("decide", 10, 20));
            let s = t.today();
            assert_eq!(s.calls, 2);
            assert_eq!(s.cache_hit, 510);
            assert_eq!(s.cache_miss, 120);
            let cats = t.by_category();
            assert_eq!(cats.len(), 2);
            assert_eq!(cats[0].0, "dialogue");
        }
        // 重新打开:从文件读回
        {
            let t = CostTracker::new(dir.clone());
            let s = t.today();
            assert_eq!(s.calls, 2);
            assert_eq!(s.cache_hit, 510);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
