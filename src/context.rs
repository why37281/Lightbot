//! 上下文组装与预算管理(缓存友好核心)。
//!
//! 组装顺序(方案由 memory.placement 决定):
//!   方案二(front):[人设][记忆说明][思考约束][摘要][记忆][历史][轨迹][提问]
//!   方案一(back): [人设][记忆说明][思考约束][摘要][历史][记忆][轨迹][提问]
//! 头部截断只删最旧消息,剩余部分逐字节不变 → DeepSeek 前缀缓存命中(设计初衷)。

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::chat::ChatCore;
use crate::config::{MemoryConfig, TrailConfig};
use crate::estimate::{compute_drop, estimate_tokens};
use crate::events::{MEMORY_GUIDE, THINKING_GUIDE};
use crate::llm::ApiMessage;
use crate::napcat::{MsgKind, ParsedMsg};
use crate::session::{HistoryMsg, Session};
use crate::trace::{self, TraceEvent};

impl ChatCore {
    /// 上下文预算管理:按记忆方案折叠 + 兜底头部截断(缓存友好)。
    pub(crate) async fn trim_context(&self, session: &mut Session, user_text: &str, turn: &str) {
        let (budget, summarize, summarize_tokens, ratio, prompt, mem_cfg, history_target, recent_cap, recent_keep) = {
            let cfg = self.cfg.read().await;
            (
                cfg.chat.context_tokens.saturating_sub(cfg.chat.reserve_tokens) as u64,
                cfg.chat.summarize,
                cfg.chat.summarize_tokens,
                cfg.chat.estimate_ratio,
                cfg.prompt().map(|p| p.prompt.clone()).unwrap_or_default(),
                cfg.chat.memory.clone(),
                cfg.chat.history_target_tokens,
                cfg.chat.recent_max_tokens as u64,
                cfg.chat.recent_keep_msgs as usize,
            )
        };
        let (prompt_tok, user_tok) = {
            // 固定前缀 token:人设 + 记忆说明 + 思考约束(恒定) + 当前提问
            let pt = (estimate_tokens(&prompt, ratio)
                + estimate_tokens(MEMORY_GUIDE, ratio)
                + estimate_tokens(THINKING_GUIDE, ratio)) as u64;
            (pt, estimate_tokens(user_text, ratio) as u64)
        };
        // 记忆:刷新 + 超预算裁剪最旧条目(保护上下文预算)
        let mut mem_tok: u64 = 0;
        if mem_cfg.enabled {
            session.memory.refresh();
            session.memory.trim_to_tokens(mem_cfg.max_tokens.max(64), ratio);
            mem_tok = session.memory.total_tokens(ratio) as u64;
        }
        let mut sum_tok = session.summary_tokens as u64;
        let hist_tok: u64 = session.history.iter().map(|h| h.tokens as u64).sum();

        // 预算报告(debug):裁剪前的占用明细
        self.log(
            "debug",
            &format!(
                "[{}] 上下文预算: 人设{prompt_tok} + 记忆{mem_tok} + 摘要{sum_tok} + 历史{hist_tok}({}条) / 总预算 {budget}t",
                session.key,
                session.history.len()
            ),
        );

        // 摘要与历史共享的预算(输入预算扣除人设、记忆与当前提问)
        let input_budget = budget.saturating_sub(prompt_tok + mem_tok + user_tok);
        if input_budget <= sum_tok && session.summary.is_some() {
            session.summary = None;
            sum_tok = 0;
            session.rewrite();
        }

        // 折叠策略:按记忆方案分派(均可关闭)
        if summarize && session.history.len() > 2 {
            let drop = if mem_cfg.placement == "front" {
                // 方案二:新历史超阈值时折叠到 ≤ recent_cap(至少保留最近 keep 条);
                // 折叠后历史从 keep 条重新积累,自然形成折叠周期(与成本模型一致)
                let tokens: Vec<u32> = session.history.iter().map(|h| h.tokens).collect();
                compute_drop(&tokens, recent_cap, recent_keep.max(1))
            } else if history_target > 0 {
                // 方案一:历史超过主动折叠阈值(或逼近预算)时折叠,至少丢 2 条
                let fold_budget = input_budget
                    .saturating_sub(sum_tok)
                    .min(history_target as u64);
                let tokens: Vec<u32> = session.history.iter().map(|h| h.tokens).collect();
                if hist_tok > fold_budget {
                    compute_drop(&tokens, fold_budget, 1).max(2)
                } else {
                    0
                }
            } else {
                0
            };
            if drop > 0 {
                self.fold_history(session, drop, summarize_tokens, ratio, turn)
                    .await;
            }
        }

        // 兜底:无论方案与开关,超出预算时必须头部截断(修复历史 bug:关闭摘要时永不截断)
        let final_budget = input_budget.saturating_sub(session.summary_tokens as u64);
        let tokens: Vec<u32> = session.history.iter().map(|h| h.tokens).collect();
        let drop = compute_drop(&tokens, final_budget, 0);
        if drop > 0 {
            session.history.drain(..drop);
            session.rewrite();
            self.log(
                "info",
                &format!("[{}] 上下文超预算,丢弃最旧 {} 条消息", session.key, drop),
            );
        }
    }

    /// 把最旧 drop 条折叠进摘要(失败则丢弃并落盘,保证磁盘与内存一致)
    pub(crate) async fn fold_history(
        &self,
        session: &mut Session,
        drop: usize,
        summarize_tokens: u32,
        ratio: f64,
        turn: &str,
    ) {
        let dropped: Vec<HistoryMsg> = session.history.drain(..drop).collect();
        let old_summary = session.summary.clone().unwrap_or_default();
        // user 消息带说话人前缀传给摘要器:折叠后「谁说的」仍然可辨
        let api_msgs: Vec<ApiMessage> = dropped
            .iter()
            .map(|h| ApiMessage {
                role: h.role.clone(),
                content: if h.role == "user" {
                    format!("{}{}", speaker_prefix(h.sender), h.text)
                } else {
                    h.text.clone()
                },
            })
            .collect();
        let model = self.cfg.read().await.active_model().cloned();
        match model {
            Some(model) => match self
                .llm
                .summarize(&model, &old_summary, &api_msgs, summarize_tokens)
                .await
            {
                Ok((s, usage)) => {
                    self.record_usage(&model, "summarize", &usage);
                    session.summary = Some(s.clone());
                    session.summary_tokens = estimate_tokens(&s, ratio);
                    session.rewrite();
                    self.trace_push(
                        &session.key,
                        &TraceEvent::Fold {
                            turn: turn.to_string(),
                            ts: trace::now_ts(),
                            folded: dropped.len(),
                            summary_tokens: session.summary_tokens,
                        },
                    )
                    .await;
                    self.log(
                        "info",
                        &format!(
                            "[{}] 已折叠 {} 条旧消息为摘要",
                            session.key,
                            dropped.len()
                        ),
                    );
                }
                Err(e) => {
                    self.log("warn", &format!("[{}] 摘要生成失败: {e}", session.key));
                    session.summary = None;
                    session.summary_tokens = 0;
                    session.rewrite();
                }
            },
            None => {
                // 无可用模型:丢弃部分也需落盘,保证磁盘与内存一致
                session.rewrite();
                self.log(
                    "info",
                    &format!(
                        "[{}] 无可用模型,直接丢弃最旧 {} 条消息",
                        session.key,
                        dropped.len()
                    ),
                );
            }
        }
    }

    /// 组装消息流(缓存友好顺序,方案由 placement 决定):
    /// 方案二(front):[人设][记忆说明][摘要][记忆][历史][轨迹][提问]
    /// 方案一(back): [人设][记忆说明][摘要][历史][记忆][轨迹][提问]
    pub(crate) async fn build_messages(
        &self,
        session: &Session,
        key: &str,
        msg: &ParsedMsg,
        prompt: &str,
        user_text: &str,
        mem_cfg: &MemoryConfig,
        trail_cfg: &TrailConfig,
        ratio: f64,
    ) -> Vec<ApiMessage> {
        let mut msgs = vec![
            ApiMessage {
                role: "system".into(),
                content: prompt.to_string(),
            },
            ApiMessage {
                role: "system".into(),
                content: MEMORY_GUIDE.to_string(),
            },
            ApiMessage {
                role: "system".into(),
                content: THINKING_GUIDE.to_string(),
            },
        ];
        if let Some(s) = &session.summary {
            msgs.push(ApiMessage {
                role: "system".into(),
                content: format!("[先前对话摘要]\n{s}"),
            });
        }
        // 记忆内容(trim_context 中已刷新与裁剪)
        let mut mem_msg: Option<ApiMessage> = None;
        if mem_cfg.enabled && !session.memory.entries.is_empty() {
            mem_msg = Some(ApiMessage {
                role: "system".into(),
                content: session.memory.system_text(),
            });
        }
        if mem_cfg.placement != "back" {
            if let Some(m) = mem_msg.clone() {
                msgs.push(m);
            }
        }
        for h in &session.history {
            msgs.push(ApiMessage {
                role: h.role.clone(),
                // user 消息带时间与说话人前缀,模型能分清谁说的、什么时候说的
                content: if h.role == "user" {
                    format!("{}{}", history_prefix(h.ts, h.sender), h.text)
                } else {
                    h.text.clone()
                },
            });
        }
        if mem_cfg.placement == "back" {
            if let Some(m) = mem_msg {
                msgs.push(m);
            }
        }
        // 群聊轨迹:最近未触发消息(位于最后,变化不影响历史/记忆缓存)。
        // 注入行为三档:
        //  window          所有完整对话都注入,按时间窗口过滤(默认);
        //  all             所有完整对话都注入,不受时间窗口限制(缓冲保留 24h);
        //  triggered_only  仅 @ / 引用触发的对话注入。
        if trail_cfg.enabled && msg.kind == MsgKind::Group {
            let inject = match trail_cfg.inject_mode.as_str() {
                "triggered_only" => msg.at_me || msg.reply_me,
                _ => true,
            };
            if inject {
                let lines = self.rt.trail_get(key);
                let window_secs = if trail_cfg.inject_mode == "window" {
                    trail_cfg.window_minutes.max(1) * 60
                } else {
                    86400
                };
                // all 模式:max_tokens 传 0 = 不设 token 上限,全部注入
                let max_tokens = if trail_cfg.inject_mode == "all" {
                    0
                } else {
                    trail_cfg.max_tokens
                };
                if let Some(content) = render_trail(&lines, window_secs, max_tokens, ratio) {
                    msgs.push(ApiMessage {
                        role: "user".into(),
                        content,
                    });
                }
            }
        }
        // 当前提问:同样带时间与说话人前缀(与历史一致,模型好对齐)
        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        msgs.push(ApiMessage {
            role: "user".into(),
            content: format!("{}{}{}", ts_label(now_ts), speaker_prefix(msg.user_id), user_text),
        });
        msgs
    }
}

/// 渲染群聊轨迹为一条 user 消息内容(窗口过滤 + 从最新截断到 max_tokens)。
/// max_tokens == 0 表示不设 token 上限(「全部注入」模式),整段缓冲全部渲染。
pub fn render_trail(
    lines: &VecDeque<(SystemTime, i64, String)>,
    window_secs: u64,
    max_tokens: u32,
    ratio: f64,
) -> Option<String> {
    let now = SystemTime::now();
    let mut rendered: Vec<String> = Vec::new();
    for (t, sender, text) in lines {
        if now
            .duration_since(*t)
            .map(|d| d.as_secs() < window_secs)
            .unwrap_or(false)
        {
            let hhmm = chrono::DateTime::<chrono::Local>::from(*t)
                .format("%m-%d %H:%M")
                .to_string();
            rendered.push(format!("[{hhmm}]{}{text}", speaker_prefix(*sender)));
        }
    }
    if rendered.is_empty() {
        return None;
    }
    // 从最新往前保留,直到 token 上限(0 = 无上限)
    let mut total = 0u32;
    let mut kept: Vec<&String> = Vec::new();
    if max_tokens == 0 {
        for line in rendered.iter().rev() {
            kept.push(line);
        }
    } else {
        for line in rendered.iter().rev() {
            total = total.saturating_add(estimate_tokens(line, ratio));
            if total > max_tokens.max(1) {
                break;
            }
            kept.push(line);
        }
    }
    if kept.is_empty() {
        return None;
    }
    let mut content = String::from("[群聊最近消息]\n");
    for line in kept.into_iter().rev() {
        content.push_str(line);
        content.push('\n');
    }
    Some(content)
}

/// 说话人前缀(注入上下文用,让模型分清谁说了什么);sender ≤ 0 时无前缀
pub fn speaker_prefix(sender: i64) -> String {
    if sender > 0 {
        format!("[QQ{sender}] ")
    } else {
        String::new()
    }
}

/// 历史消息注入前缀:时间(何时说的)+ 说话人(谁说的);ts ≤ 0(旧记录)时省略时间
pub fn history_prefix(ts: i64, sender: i64) -> String {
    format!("{}{}", ts_label(ts), speaker_prefix(sender))
}

/// epoch 秒 → "[MM-DD HH:MM] "(本地时区);非法时间返回空串
pub fn ts_label(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|d| {
            chrono::DateTime::<chrono::Local>::from(d)
                .format("[%m-%d %H:%M] ")
                .to_string()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

        #[test]
        fn trail_render() {
            let now = SystemTime::now();
            let lines = vec![
                (now, 123, "你好".to_string()),
                (now, 456, "令牌:abc".to_string()),
            ];
            let out = render_trail(&VecDeque::from(lines.clone()), 300, 800, 1.0).unwrap();
            assert!(out.starts_with("[群聊最近消息]"));
            assert!(out.contains("你好"));
            assert!(out.contains("令牌"));
            assert!(out.contains("[QQ123]")); // 说话人前缀(模型能分清谁说的)
            assert!(out.contains('[')); // 时间戳格式 [HH:MM]
            // token 上限较小:只保留最新一条(旧的一条放不下)
            let out2 = render_trail(&VecDeque::from(lines.clone()), 300, 10, 1.0).unwrap();
            assert!(out2.contains("令牌"));
            assert!(!out2.contains("你好"));
            // max_tokens = 0:不设上限(「全部注入」模式),整段缓冲全部保留
            let out3 = render_trail(&VecDeque::from(lines), 300, 0, 1.0).unwrap();
            assert!(out3.contains("你好"));
            assert!(out3.contains("令牌"));
            // 过期消息被过滤
            let old = now - Duration::from_secs(600);
            let lines2 = vec![(old, 123, "过期".to_string())];
            assert!(render_trail(&VecDeque::from(lines2), 300, 800, 1.0).is_none());
        }
}
