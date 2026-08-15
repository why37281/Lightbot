//! OpenAI 兼容 LLM 客户端(DeepSeek / 通义 / OpenAI 等)。
//!
//! - 完整对话走 **流式(SSE)**:思考/正文增量实时上抛(会话详情页直播、状态灯、思考提示计时),
//!   请求可被用户中止;
//! - 决策 / 摘要 / 连通测试走非流式(小请求,无需直播);
//! - 采集 usage 中的 prompt_cache_hit_tokens / prompt_cache_miss_tokens 用于缓存命中率展示。

use std::time::Duration;

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::config::ModelConfig;

/// 用户主动停止的固定错误文案(chat.rs 据此决定不向 QQ 发送错误)
pub const USER_STOPPED: &str = "用户已停止本次回复";

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
    /// 思维链 token 数(DeepSeek 思考模式: completion_tokens_details.reasoning_tokens)
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
    /// 推理模型的思考内容(reasoning_content)
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
                .timeout(Duration::from_secs(600))
                .build()
                .expect("failed to build http client"),
        }
    }

    /// 构造请求体(流式/非流式共用;修复历史 bug:max_tokens 应为 min 而非 max,
    /// 否则每次请求都按模型上限申请输出,突破回复预留)
    fn build_body(
        m: &ModelConfig,
        messages: &[ApiMessage],
        max_tokens: Option<u32>,
        stream: bool,
    ) -> Value {
        let mut body = json!({
            "model": m.model,
            "messages": messages,
            "stream": stream,
        });
        // DeepSeek V4:思考模式由 thinking 参数切换(默认开启),思考模式下不支持 temperature
        // 非 DeepSeek 模型不发送 thinking 参数,保持通用兼容
        let is_deepseek = m.model.to_lowercase().contains("deepseek");
        if is_deepseek && m.thinking != "disabled" {
            body["thinking"] = json!({ "type": "enabled" });
            if !m.reasoning_effort.is_empty() {
                body["reasoning_effort"] = json!(m.reasoning_effort);
            }
        } else {
            body["temperature"] = json!(m.temperature);
            if is_deepseek && m.thinking == "disabled" {
                body["thinking"] = json!({ "type": "disabled" });
            }
        }
        if let Some(t) = max_tokens {
            body["max_tokens"] = json!(t.min(m.max_tokens));
        }
        body
    }

    fn parse_reply(v: &Value, raw: &str) -> Result<LlmReply, String> {
        let choice = &v["choices"][0];
        let msg = &choice["message"];
        let text = msg["content"].as_str().unwrap_or("").trim().to_string();
        let reasoning = msg["reasoning_content"].as_str().unwrap_or("").trim().to_string();
        if text.is_empty() && reasoning.is_empty() {
            return Err(format!("模型返回空内容: {}", truncate(raw, 300)));
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

    /// 非流式对话补全(决策/摘要/连通测试用)。
    /// messages 应已按缓存友好顺序排好。
    pub async fn chat(
        &self,
        m: &ModelConfig,
        messages: &[ApiMessage],
        max_tokens: Option<u32>,
    ) -> Result<LlmReply, String> {
        let url = format!("{}/chat/completions", m.base_url.trim_end_matches('/'));
        let body = Self::build_body(m, messages, max_tokens, false);
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
        Self::parse_reply(&v, &text)
    }

    /// 流式对话补全(完整通道)。返回后可逐事件驱动,期间响应 cancel 通道。
    pub async fn chat_stream(
        &self,
        m: &ModelConfig,
        messages: &[ApiMessage],
        max_tokens: Option<u32>,
        cancel: watch::Receiver<bool>,
    ) -> Result<StreamedChat, String> {
        let url = format!("{}/chat/completions", m.base_url.trim_end_matches('/'));
        let mut body = Self::build_body(m, messages, max_tokens, true);
        // 请求在最后一个数据块返回 usage(OpenAI 兼容惯例;DeepSeek 默认也会返回)
        body["stream_options"] = json!({ "include_usage": true });
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
        if !status.is_success() {
            let text = resp
                .text()
                .await
                .unwrap_or_else(|_| String::new());
            return Err(format!("HTTP {status}: {}", truncate(&text, 300)));
        }
        Ok(StreamedChat {
            stream: Box::pin(
                resp.bytes_stream()
                    .map(|r| r.map_err(|e| format!("流式读取失败: {e}"))),
            ),
            buffer: String::new(),
            cancel,
            text: String::new(),
            reasoning: String::new(),
            usage: Usage::default(),
            pending_content: None,
        })
    }

    /// 查询账户余额(DeepSeek `GET /user/balance`,Bearer 认证)。
    /// 返回原始 JSON(`is_available` + `balance_infos`);非 DeepSeek 服务商无此接口会报错。
    pub async fn balance(&self, m: &ModelConfig) -> Result<Value, String> {
        let url = format!("{}/user/balance", m.base_url.trim_end_matches('/'));
        let timeout = Duration::from_secs(m.timeout_secs.max(10));
        let resp = self
            .http
            .get(&url)
            .timeout(timeout)
            .bearer_auth(m.api_key.trim())
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|e| format!("余额查询失败: {e}"))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| format!("读取响应失败: {e}"))?;
        if !status.is_success() {
            return Err(format!("HTTP {status}: {}", truncate(&text, 200)));
        }
        serde_json::from_str(&text).map_err(|e| format!("响应解析失败: {e}"))
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

    /// 用当前模型把一批旧消息压缩成摘要(缓存友好折叠,非思考模式)。
    /// 返回(摘要文本, usage) —— usage 供费用追踪。
    /// 摘要必须保留「谁说的」:历史消息的 AI/用户归属在折叠后仍然可辨。
    pub async fn summarize(
        &self,
        m: &ModelConfig,
        old_summary: &str,
        dropped: &[ApiMessage],
        max_tokens: u32,
    ) -> Result<(String, Usage), String> {
        let mut content = String::from("请把下面的对话历史压缩成简洁的要点摘要,保留:关键事实、用户的偏好与要求、尚未完成的事项、对话主题。\n每条要点必须注明是谁说的:用户说的标「用户:」,AI 说的标「AI:」。\n只输出摘要本身。\n\n");
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
        Ok((reply.text, reply.usage))
    }

    /// 决策请求:判断这条消息是否需要回复。
    /// 思考模式与推理强度跟随模型配置(先思考,再输出字母);仅输出上限 32 tokens。
    /// 上下文只带人设 + 当前消息(附触发说明),不带历史(决策只看当下值不值得回)。
    /// 返回(结论, usage)。
    pub async fn decide(
        &self,
        m: &ModelConfig,
        prompt: &str,
        text: &str,
        trigger_hint: &str,
        sender: i64,
    ) -> Result<(bool, Usage), String> {
        let mut m2 = m.clone();
        m2.max_tokens = 32;
        let msgs = build_decide_messages(prompt, text, trigger_hint, sender);
        let reply = self.chat(&m2, &msgs, Some(32)).await?;
        Ok((parse_decision(&reply.text), reply.usage))
    }
}

/// 流式响应的增量事件
pub enum StreamEvent {
    /// 思考增量(reasoning_content)
    Reasoning { delta: String },
    /// 正文增量(content)
    Content { delta: String },
}

/// 流式响应驱动器:逐事件产出增量,累积全文与 usage。
pub struct StreamedChat {
    stream: std::pin::Pin<
        Box<dyn futures_util::Stream<Item = Result<bytes::Bytes, String>> + Send>,
    >,
    buffer: String,
    cancel: watch::Receiver<bool>,
    text: String,
    reasoning: String,
    usage: Usage,
    /// 同一 delta 中 reasoning 与 content 并存时暂存的正文增量
    pending_content: Option<String>,
}

impl StreamedChat {
    /// 下一个增量事件;None 表示流结束(用 finish() 取全文)。
    pub async fn next_event(&mut self) -> Option<Result<StreamEvent, String>> {
        use futures_util::StreamExt;
        loop {
            if *self.cancel.borrow() {
                return Some(Err(USER_STOPPED.into()));
            }
            if let Some(c) = self.pending_content.take() {
                self.text.push_str(&c);
                return Some(Ok(StreamEvent::Content { delta: c }));
            }
            // 缓冲区里已有完整行则先消费
            if let Some(pos) = self.buffer.find('\n') {
                let line: String = self.buffer.drain(..=pos).collect();
                let line = line.trim();
                if let Some(data) = line.strip_prefix("data:") {
                    let data = data.trim();
                    if data == "[DONE]" {
                        return None;
                    }
                    if let Ok(v) = serde_json::from_str::<Value>(data) {
                        if let Some(ev) = self.apply_chunk(&v) {
                            return Some(Ok(ev));
                        }
                    }
                    // 无增量的块(如仅含 usage)继续读下一行
                }
                continue;
            }
            // 拉取下一段网络数据
            let chunk = self.stream.next().await?;
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => return Some(Err(e)),
            };
            self.buffer.push_str(&String::from_utf8_lossy(&chunk));
        }
    }

    /// 应用一个 data 块:累积增量与 usage,返回增量事件(如有)
    fn apply_chunk(&mut self, v: &Value) -> Option<StreamEvent> {
        let delta = &v["choices"][0]["delta"];
        if let Some(usage) = v["usage"].as_object() {
            self.usage = Usage {
                prompt_tokens: usage
                    .get("prompt_tokens")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0),
                completion_tokens: usage
                    .get("completion_tokens")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0),
                cache_hit: usage
                    .get("prompt_cache_hit_tokens")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0),
                cache_miss: usage
                    .get("prompt_cache_miss_tokens")
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0),
                reasoning_tokens: usage
                    .get("completion_tokens_details")
                    .and_then(|x| x.get("reasoning_tokens"))
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0),
            };
        }
        if let Some(r) = delta["reasoning_content"].as_str() {
            if !r.is_empty() {
                self.reasoning.push_str(r);
                // 同一 delta 中可能同时带正文增量:先存起来,下轮返回,绝不丢弃
                if let Some(c) = delta["content"].as_str() {
                    if !c.is_empty() {
                        self.pending_content = Some(c.to_string());
                    }
                }
                return Some(StreamEvent::Reasoning {
                    delta: r.to_string(),
                });
            }
        }
        if let Some(c) = delta["content"].as_str() {
            if !c.is_empty() {
                self.text.push_str(c);
                return Some(StreamEvent::Content {
                    delta: c.to_string(),
                });
            }
        }
        None
    }

    /// 取最终结果(应在 next_event 返回 None 后调用)
    pub fn finish(self) -> LlmReply {
        LlmReply {
            text: self.text.trim().to_string(),
            reasoning: self.reasoning.trim().to_string(),
            usage: self.usage,
        }
    }
}

/// 构造决策请求的消息(独立函数:便于测试与定位决策提示词)。
///
/// 关键设计:用户消息带「发送者」与「触发说明」——决策器必须知道消息是谁发的、
/// 是怎么触发机器人的(@ / 引用 / 私聊 / 关键词 / 称呼 / 插话采样),否则 @ 段被剥掉后
/// 它只会看到裸文本,把所有消息都误判为「纯闲聊」。
pub fn build_decide_messages(
    prompt: &str,
    text: &str,
    trigger_hint: &str,
    sender: i64,
) -> Vec<ApiMessage> {
    let mut user = String::new();
    if sender > 0 {
        user.push_str(&format!("【发送者 QQ:{sender}】\n"));
    }
    user.push_str(&format!("【触发说明:{trigger_hint}】\n{text}"));
    vec![
        ApiMessage {
            role: "system".into(),
            content: format!(
                "{prompt}\n\n(你是这个群里的一员,下面是可能触发你回复的消息。判断你是否需要回复:\n\
                 - 消息 @ 了你、引用回复了你、是发给你的私聊、明确提问/求助/提到你的称呼 → 需要回复;\n\
                 - 纯闲聊、与你无关、无需回应 → 不需要回复。\n\
                 思考过程请保持简短:只需想清楚是否需要回复、以及如果需要的话大致回复什么即可。\n\
                 只输出一个字母:需要回复输出 Y,不需要输出 N。)"
            ),
        },
        ApiMessage {
            role: "user".into(),
            content: user,
        },
    ]
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

    #[test]
    fn decide_messages_include_trigger_hint() {
        let msgs = build_decide_messages("人设", "你好", "对方 @ 了你", 67890);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].role, "system");
        assert!(msgs[0].content.contains("人设"));
        assert!(msgs[0].content.contains("需要回复"));
        assert!(msgs[1].content.starts_with("【发送者 QQ:67890】"));
        assert!(msgs[1].content.contains("【触发说明:对方 @ 了你】"));
        assert!(msgs[1].content.contains("你好"));
        // 未知发送者(0)不注入发送者行
        let msgs2 = build_decide_messages("人设", "hi", "私聊", 0);
        assert!(!msgs2[1].content.contains("发送者"));
    }

    #[test]
    fn max_tokens_is_capped() {
        let mut m = ModelConfig::default();
        m.max_tokens = 8192;
        let body = LlmClient::build_body(
            &m,
            &[ApiMessage {
                role: "user".into(),
                content: "hi".into(),
            }],
            Some(1024),
            false,
        );
        assert_eq!(body["max_tokens"], 1024);
        // 请求值超过模型上限时封顶到模型上限
        let body2 = LlmClient::build_body(
            &m,
            &[ApiMessage {
                role: "user".into(),
                content: "hi".into(),
            }],
            Some(20000),
            false,
        );
        assert_eq!(body2["max_tokens"], 8192);
    }

    #[test]
    fn sse_chunks_applied() {
        let (_tx, rx) = watch::channel(false);
        // 直接测 apply_chunk 逻辑
        let mut sc = StreamedChat {
            stream: Box::pin(futures_util::stream::empty()),
            buffer: String::new(),
            cancel: rx,
            text: String::new(),
            reasoning: String::new(),
            usage: Usage::default(),
            pending_content: None,
        };
        let v = serde_json::json!({
            "choices": [{"delta": {"reasoning_content": "想", "content": ""}}]
        });
        match sc.apply_chunk(&v) {
            Some(StreamEvent::Reasoning { delta }) => assert_eq!(delta, "想"),
            _ => panic!("应为思考增量"),
        }
        let v2 = serde_json::json!({
            "choices": [{"delta": {"content": "你好"}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 10,
                      "prompt_cache_hit_tokens": 90, "prompt_cache_miss_tokens": 10}
        });
        match sc.apply_chunk(&v2) {
            Some(StreamEvent::Content { delta }) => assert_eq!(delta, "你好"),
            _ => panic!("应为正文增量"),
        }
        assert_eq!(sc.usage.prompt_tokens, 100);
        assert_eq!(sc.usage.cache_hit, 90);
        assert_eq!(sc.reasoning, "想");
        assert_eq!(sc.text, "你好");

        // 同一 delta 同时含思考与正文:正文不得丢失(暂存待下轮)
        let v3 = serde_json::json!({
            "choices": [{"delta": {"reasoning_content": "再想", "content": "接着写"}}]
        });
        match sc.apply_chunk(&v3) {
            Some(StreamEvent::Reasoning { delta }) => assert_eq!(delta, "再想"),
            _ => panic!("应为思考增量"),
        }
        assert_eq!(sc.pending_content.as_deref(), Some("接着写"));
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
