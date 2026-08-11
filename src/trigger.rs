//! 触发引擎:被动规则 + 软 at + 插话采样器。
//!
//! 纯函数设计,便于单元测试:
//! - `passive_hit`: 现有的 @ / 回复引用 / 关键词 / 私聊 触发(完整通道)
//! - `soft_at_hit`: 消息中提到机器人称呼(软 at,完整通道,必回)
//! - `interject_probability` + `sample`: 插话概率采样(轻量通道)
//!
//! 插话采样公式:
//!   概率 = clamp(基线 + 钩子词 +0.15 - 纯水 -0.03, 0.0, 0.6)
//!   再按模式缩放: fixed 不缩放; adaptive 乘群活跃度因子(刷屏 ×0.3, 冷清 ×2)

use crate::config::Config;
use crate::napcat::{MsgKind, ParsedMsg};

/// 被动触发(现状规则):@ / 回复引用 / 关键词 / 私聊直接回
pub fn passive_hit(cfg: &Config, msg: &ParsedMsg) -> bool {
    match msg.kind {
        MsgKind::Private => cfg.chat.enable_private,
        MsgKind::Group => {
            if !cfg.chat.enable_group {
                return false;
            }
            // 关键词:逗号分隔列表,任一命中(包含即触发)
            let kw_hit = contains_any(&msg.text, &cfg.napcat.keyword);
            match cfg.napcat.group_trigger.as_str() {
                "at" => msg.at_me || (cfg.napcat.reply_quoted && msg.reply_me),
                "keyword" => kw_hit,
                _ => msg.at_me || (cfg.napcat.reply_quoted && msg.reply_me) || kw_hit,
            }
        }
    }
}

/// 软 at:消息中提到机器人称呼(逗号分隔列表)。仅群聊使用。
pub fn soft_at_hit(cfg: &Config, text: &str) -> bool {
    if !cfg.chat.interject.soft_at_reply {
        return false;
    }
    contains_any(text, &cfg.chat.interject.names)
}

/// 纯水消息:空 / 单字 / 过短 / 纯表情占位 / 纯标点。命中则降低插话概率。
pub fn is_water(text: &str) -> bool {
    let t = text.trim();
    if t.is_empty() {
        return true;
    }
    let chars: Vec<char> = t.chars().collect();
    if chars.len() <= 2 {
        return true;
    }
    // 纯表情/图片占位(去占位符后为空)
    let stripped: String = t
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .replace("[表情]", "")
        .replace("[图片]", "")
        .replace("[戳一戳]", "");
    let stripped = stripped.trim();
    if stripped.is_empty() {
        return true;
    }
    stripped
        .chars()
        .all(|c| c.is_ascii_punctuation() || c.is_whitespace() || is_cjk_punct(c))
}

/// 常见中文/全角标点(Unicode 通用类别判断尚未稳定,手动覆盖常用范围)
fn is_cjk_punct(c: char) -> bool {
    let u = c as u32;
    (0x3000..=0x303F).contains(&u) // CJK 符号和标点(。、《》…)
        || (0xFF00..=0xFFEF).contains(&u) // 全角形式(，。！？：；)
        || (0x2018..=0x201F).contains(&u) // 各种引号
        || matches!(u, 0x2014 | 0x2026 | 0x00B7) // —— … ·
}

/// 钩子词加分:命中任一钩子词返回 0.15,否则 0
pub fn hook_bonus(cfg: &Config, text: &str) -> f64 {
    if contains_any(text, &cfg.chat.interject.hooks) {
        0.15
    } else {
        0.0
    }
}

/// 插话概率(0.0~0.6)。activity_factor 由调用方按群活跃度计算:
/// fixed 模式下忽略,adaptive 模式下相乘。
pub fn interject_probability(cfg: &Config, text: &str, activity_factor: f64) -> f64 {
    if !cfg.chat.interject.enabled {
        return 0.0;
    }
    let mut p = cfg.chat.interject.base_probability.clamp(0.0, 1.0);
    if is_water(text) {
        p -= 0.03;
    } else {
        p += hook_bonus(cfg, text);
    }
    let p = p.clamp(0.0, 0.6);
    match cfg.chat.interject.mode.as_str() {
        "fixed" => p,
        _ => p * activity_factor,
    }
}

/// 概率采样:rand01 为 [0,1) 均匀随机数
pub fn sample(rand01: f64, prob: f64) -> bool {
    rand01 < prob.clamp(0.0, 1.0)
}

/// 群活跃度因子(钟形曲线):按归一化消息速率(条/分钟)映射。
/// 中间高、两端低 —— 有人在聊且节奏不快时参与感最强(插话有人回应);
/// 冷场没人看(避免自言自语)与刷屏(被淹没/打断节奏)都降低。
pub fn activity_factor(rate_per_min: f64) -> f64 {
    if rate_per_min < 0.2 {
        0.1 // 近乎无人:不插,避免自言自语
    } else if rate_per_min < 1.0 {
        0.4 // 很冷清:偶尔说一句
    } else if rate_per_min < 5.0 {
        1.6 // ★ 参与感最强:有人聊、节奏不快
    } else if rate_per_min < 15.0 {
        1.0 // 活跃:正常参与
    } else {
        0.5 // 刷屏:说了被淹没,也打断节奏
    }
}

/// 剥离关键词前缀(触发用;不匹配则原文返回)。支持逗号分隔多关键词,
/// 任一关键词命中前缀即剥离(前缀后残留的空格一并去掉)。
pub fn strip_keyword<'a>(text: &'a str, kw: &str) -> &'a str {
    for k in kw.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some(rest) = text.strip_prefix(k) {
            return rest.trim_start();
        }
    }
    text
}

/// 逗号分隔列表是否命中文本(任一项非空且被包含)
fn contains_any(text: &str, list: &str) -> bool {
    list.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .any(|item| text.contains(item))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use serde_json::json;

    fn msg_group(text: &str) -> ParsedMsg {
        let v = json!({
            "message_type": "group",
            "group_id": 1,
            "user_id": 2,
            "message": [{"type": "text", "data": {"text": text}}]
        });
        crate::napcat::parse_message(&v, Some(10001))
    }

    #[test]
    fn soft_at_detects_names() {
        let cfg = Config::default();
        assert!(soft_at_hit(&cfg, "小灯你怎么看"));
        assert!(!soft_at_hit(&cfg, "灯宝在吗")); // 未配置的称呼不命中
        assert!(!soft_at_hit(&cfg, "今天天气不错"));
        // 配置多称呼后命中
        let mut cfg2 = cfg.clone();
        cfg2.chat.interject.names = "小灯,灯宝".into();
        assert!(soft_at_hit(&cfg2, "灯宝在吗"));
        // 关闭后不命中
        cfg2.chat.interject.soft_at_reply = false;
        assert!(!soft_at_hit(&cfg2, "小灯你怎么看"));
    }

    #[test]
    fn water_detection() {
        assert!(is_water("6"));
        assert!(is_water("哈哈"));
        assert!(is_water("[表情] [表情]"));
        assert!(is_water("。。。"));
        assert!(!is_water("今天吃什么"));
        assert!(!is_water("哈哈哈哈哈哈"));
    }

    #[test]
    fn probability_math() {
        let mut cfg = Config::default();
        cfg.chat.interject.base_probability = 0.05;
        // 普通消息 + 自适应 ×1
        let p = interject_probability(&cfg, "今天天气不错", 1.0);
        assert!((p - 0.05).abs() < 1e-9);
        // 钩子词加分
        let p2 = interject_probability(&cfg, "大家觉得这个怎么样", 1.0);
        assert!((p2 - 0.20).abs() < 1e-9);
        // 水消息减分(长度<=2)
        let p3 = interject_probability(&cfg, "哈哈", 1.0);
        assert!((p3 - 0.02).abs() < 1e-9);
        // 自适应缩放
        let p4 = interject_probability(&cfg, "今天天气不错", 2.0);
        assert!((p4 - 0.10).abs() < 1e-9);
        // 关闭后为 0
        cfg.chat.interject.enabled = false;
        assert_eq!(interject_probability(&cfg, "今天天气不错", 1.0), 0.0);
    }

    #[test]
    fn sampling_boundaries() {
        assert!(sample(0.04, 0.05)); // 0.04 < 0.05
        assert!(!sample(0.05, 0.05)); // 边界:rand >= prob 不中
        assert!(!sample(0.999, 0.6)); // 0.999 > 0.6 不中
        assert!(!sample(0.0, 0.0));
    }

    #[test]
    fn activity_scaling() {
        // 钟形曲线:两端低、中间高
        assert_eq!(activity_factor(0.1), 0.1); // 近乎无人
        assert_eq!(activity_factor(0.5), 0.4); // 很冷清
        assert_eq!(activity_factor(3.0), 1.6); // ★ 参与感最强
        assert_eq!(activity_factor(8.0), 1.0); // 活跃
        assert_eq!(activity_factor(20.0), 0.5); // 刷屏
        // 边界
        assert_eq!(activity_factor(0.2), 0.4);
        assert_eq!(activity_factor(1.0), 1.6);
        assert_eq!(activity_factor(5.0), 1.0);
        assert_eq!(activity_factor(15.0), 0.5);
    }

    #[test]
    fn passive_rules_unchanged() {
        let cfg = Config::default();
        // 群聊 at 触发
        let mut m = msg_group("你好");
        m.at_me = true;
        assert!(passive_hit(&cfg, &m));
        // 私聊直接回
        let v = json!({
            "message_type": "private",
            "user_id": 2,
            "message": [{"type": "text", "data": {"text": "hi"}}]
        });
        let pm = crate::napcat::parse_message(&v, Some(10001));
        assert!(passive_hit(&cfg, &pm));
        // 群聊无 at 无关键词不触发
        let m2 = msg_group("今天天气不错");
        assert!(!passive_hit(&cfg, &m2));
    }

    #[test]
    fn strip_keyword_works() {
        let kw = "小灯 ";
        assert_eq!(strip_keyword("小灯 你好", kw), "你好");
        assert_eq!(strip_keyword("你好", kw), "你好");
        // 多关键词
        assert_eq!(strip_keyword("机器人 你好", "小灯,机器人"), "你好");
        assert_eq!(strip_keyword("小灯你好", "小灯, 机器人"), "你好");
        assert_eq!(strip_keyword("灯你好", "小灯,机器人"), "灯你好");
    }

    #[test]
    fn keyword_list_multi() {
        let mut cfg = Config::default();
        cfg.napcat.group_trigger = "keyword".into();
        cfg.napcat.keyword = "小灯,机器人".into();
        // 任一关键词包含即触发
        assert!(passive_hit(&cfg, &msg_group("机器人今天吃什么")));
        assert!(passive_hit(&cfg, &msg_group("小灯在吗")));
        // 不匹配不触发
        assert!(!passive_hit(&cfg, &msg_group("今天吃什么")));
        // 空关键词永不触发
        cfg.napcat.keyword = "  , ".into();
        assert!(!passive_hit(&cfg, &msg_group("小灯在吗")));
    }
}
