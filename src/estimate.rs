//! 轻量 token 估算与预算工具(无依赖底层模块)。
//!
//! 字符级估算避免引入 tokenizer 依赖;`estimate_ratio`(默认 1.15)提供保守系数,
//! 宁可高估不可低估,防止上下文超预算被服务商截断。

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
}
