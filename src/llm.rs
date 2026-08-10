//! OpenAI 兼容 LLM 客户端(DeepSeek / 通义 / OpenAI 等)。
//! 重点采集 usage 中的 prompt_cache_hit_tokens / prompt_cache_miss_tokens,
//! 用于在 GUI 展示上下文缓存命中率(DeepSeek 自动 context caching)。

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::config::ModelConfig;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ApiMessage {
    pub role: String,
    pub content: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// 命中上下文缓存的前缀 token(DeepSeek 计费极低)
    pub cache_hit: u64,
    /// 未命中缓存的 token
    pub cache_miss: u64,
    /// 思维链 token 数(DeepSeek V4 思考模式: completion_tokens_details.reasoning_tokens)
    pub reasoning_tokens: u64,
}

impl Usage {
    /// 缓存命中率(0.0 ~ 1.0)
    pub fn hit_ratio(&self) -> f64 {
        let total = self.cache_hit + self.cache_miss;
        if total == 0 {
            0.0
        } else {
            self.cache_hit as f64 / total as f64
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LlmReply {
    pub text: String,
    /// 推理模型的思考内容(reasoning_content),单独返回便于前端展示
    pub reasoning: String,
    pub usage: Usage,
}

#[derive(Clone)]
pub struct LlmClient {
    http: reqwest::Client,
}

impl LlmClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(300))
                .build()
                .expect("failed to build http client"),
        }
    }

    /// 非流式对话补全。messages 应已按缓存友好顺序排好:
    /// [system(人设), system(摘要)?, 历史..., 当前提问]
    pub async fn chat(
        &self,
        m: &ModelConfig,
        messages: &[ApiMessage],
        max_tokens: Option<u32>,
    ) -> Result<LlmReply, String> {
        let url = format!("{}/chat/completions", m.base_url.trim_end_matches('/'));
        let mut body = json!({
            "model": m.model,
            "messages": messages,
            "stream": false,
        });
        // DeepSeek V4:思考模式由 thinking 参数切换(默认开启),思考模式下不支持 temperature
        // 非 DeepSeek 模型不发送 thinking 参数,保持通用兼容
        let is_deepseek = m.model.to_lowercase().contains("deepseek");
        if is_deepseek && m.thinking != "disabled" {
            body["thinking"] = json!({"type": "enabled"});
            if !m.reasoning_effort.is_empty() {
                body["reasoning_effort"] = json!(m.reasoning_effort);
            }
        } else {
            body["temperature"] = json!(m.temperature);
            if is_deepseek && m.thinking == "disabled" {
                body["thinking"] = json!({"type": "disabled"});
            }
        }
        if let Some(t) = max_tokens {
            body["max_tokens"] = json!(t.max(m.max_tokens));
        }

        let timeout = Duration::from_secs(m.timeout_secs.max(10));
        let resp = self
            .http
            .post(&url)
            .timeout(timeout)
            .bearer_auth(m.api_key.trim())
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("请求失败: {e}"))?;

        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("读取响应失败: {e}"))?;
        if !status.is_success() {
            return Err(format!("HTTP {status}: {}", truncate(&text, 300)));
        }

        let v: Value = serde_json::from_str(&text)
            .map_err(|e| format!("响应解析失败: {e}\n{}", truncate(&text, 300)))?;

        let choice = &v["choices"][0];
        let msg = &choice["message"];
        let text = msg["content"].as_str().unwrap_or("").trim().to_string();
        let reasoning = msg["reasoning_content"].as_str().unwrap_or("").trim().to_string();

        if text.is_empty() && reasoning.is_empty() {
            return Err(format!("模型返回空内容: {}", truncate(&text, 300)));
        }

        let usage = Usage {
            prompt_tokens: v["usage"]["prompt_tokens"].as_u64().unwrap_or(0),
            completion_tokens: v["usage"]["completion_tokens"].as_u64().unwrap_or(0),
            cache_hit: v["usage"]["prompt_cache_hit_tokens"].as_u64().unwrap_or(0),
            cache_miss: v["usage"]["prompt_cache_miss_tokens"].as_u64().unwrap_or(0),
            reasoning_tokens: v["usage"]["completion_tokens_details"]["reasoning_tokens"]
                .as_u64()
                .unwrap_or(0),
        };

        Ok(LlmReply {
            text,
            reasoning,
            usage,
        })
    }

    /// 连通性测试:发一条最小请求(关闭思考模式,快速返回)
    pub async fn ping(&self, m: &ModelConfig) -> Result<String, String> {
        let msgs = vec![ApiMessage {
            role: "user".into(),
            content: "回复「ok」两个字".into(),
        }];
        let mut m2 = m.clone();
        m2.thinking = "disabled".into();
        let start = std::time::Instant::now();
        let reply = self.chat(&m2, &msgs, Some(16)).await?;
        Ok(format!(
            "✓ 连通,模型回复:{} (耗时 {}ms)",
            truncate(&reply.text, 50),
            start.elapsed().as_millis()
        ))
    }

    /// 用当前模型把一批旧消息压缩成摘要(缓存友好折叠,非思考模式)
    pub async fn summarize(
        &self,
        m: &ModelConfig,
        old_summary: &str,
        dropped: &[ApiMessage],
        max_tokens: u32,
    ) -> Result<String, String> {
        let mut content = String::from("请把下面的对话历史压缩成简洁的要点摘要,保留:关键事实、用户的偏好与要求、尚未完成的事项、对话主题。只输出摘要本身。\n\n");
        if !old_summary.is_empty() {
            content.push_str("【已有摘要】\n");
            content.push_str(old_summary);
            content.push_str("\n\n");
        }
        content.push_str("【新增对话】\n");
        for d in dropped {
            let role = if d.role == "assistant" { "AI" } else { "用户" };
            content.push_str(&format!("{role}: {}\n", truncate(&d.content, 600)));
        }
        let msgs = vec![ApiMessage {
            role: "user".into(),
            content,
        }];
        // 摘要生成无需思考模式,更快更省
        let mut m2 = m.clone();
        m2.thinking = "disabled".into();
        let reply = self
            .chat(&m2, &msgs, Some(max_tokens.max(64)))
            .await
            .map_err(|e| format!("摘要生成失败: {e}"))?;
        Ok(reply.text)
    }

    /// 决策请求:判断这条消息是否需要回复。
    /// 用当前模型,但强制关闭思考模式、极小输出(16 tokens),开销接近一次 ping。
    /// 上下文只带人设 + 当前消息,不带历史(决策只看当下值不值得回)。
    pub async fn decide(&self, m: &ModelConfig, prompt: &str, text: &str) -> Result<bool, String> {
        let mut m2 = m.clone();
        m2.thinking = "disabled".into();
        m2.max_tokens = 16;
        let msgs = vec![
            ApiMessage {
                role: "system".into(),
                content: format!(
                    "{prompt}\n\n(你是这个群里的一员。判断下面这条消息是否需要你回复:\n\
                     - 被点名、提问、求助、@你 → 需要回复\n\
                     - 纯闲聊、与你无关、无需回应 → 不需要回复\n\
                     只输出一个字母:需要回复输出 Y,不需要输出 N。)"
                ),
            },
            ApiMessage {
                role: "user".into(),
                content: text.to_string(),
            },
        ];
        let reply = self.chat(&m2, &msgs, Some(16)).await?;
        Ok(parse_decision(&reply.text))
    }
}

/// 解析决策输出:Y/y → 回复;N/n → 不回复;其他(乱输出)→ 保守回复
pub fn parse_decision(text: &str) -> bool {
    match text.trim().chars().next() {
        Some('Y') | Some('y') => true,
        Some('N') | Some('n') => false,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_parsing() {
        assert!(parse_decision("Y"));
        assert!(parse_decision("y"));
        assert!(!parse_decision("N"));
        assert!(!parse_decision("n"));
        assert!(!parse_decision("N, 不需要"));
        assert!(parse_decision(""));
        assert!(parse_decision("我不知道"));
        assert!(parse_decision("  Y  "));
    }
}

/// 截断长文本(用于错误信息与日志)
fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let t: String = s.chars().take(n).collect();
        format!("{t}…")
    }
}
