//! QQ 内置斜杠命令(/clear /stats /remember /forget /memories /model /prompt)。
//!
//! ⚠️ 锁纪律:先快照所需配置再执行;写锁只在无读锁时获取
//! (历史 bug:/model、/prompt 持读锁跨写锁 await,造成死锁)。

use crate::chat::ChatCore;
use crate::napcat::{MsgKind, ParsedMsg};
use crate::session::Session;

impl ChatCore {
    /// 内置斜杠命令,返回 Some(回复文本) 表示已处理。
    /// ⚠️ 锁纪律:先快照所需配置再执行;写锁只在无读锁时获取
    /// (历史 bug:/model、/prompt 持读锁跨写锁 await,造成死锁)。
    pub(crate) async fn handle_command(&self, session: &mut Session, msg: &ParsedMsg, text: &str) -> Option<String> {
        let (model_names, prompt_ids, mem_cfg) = {
            let cfg = self.cfg.read().await;
            (
                cfg.models.iter().map(|m| m.name.clone()).collect::<Vec<_>>(),
                cfg.prompts.iter().map(|p| p.id.clone()).collect::<Vec<_>>(),
                cfg.chat.memory.clone(),
            )
        };
        let reply: String = match text {
            "/clear" => {
                session.clear();
                // 关键修复:清空上下文必须同时清空群聊轨迹,否则未触发消息
                // (如发过的令牌)仍会通过轨迹注入,导致「/clear 之后机器人还记得」
                self.rt.trail_remove(&session.key);
                self.emit_session(&session.key, session).await;
                self.log("info", &format!("[{}] 已清空上下文与群聊轨迹", session.key));
                "🧹 上下文与群聊轨迹已清空,重新开始对话。".to_string()
            }
            "/stats" => {
                let toks = session.total_tokens();
                self.log(
                    "info",
                    &format!(
                        "[{}] 查看统计: {} 条消息,约 {} tokens",
                        session.key,
                        session.history.len(),
                        toks
                    ),
                );
                format!(
                    "📊 当前会话:{} 条消息,约 {} tokens{}",
                    session.history.len(),
                    toks,
                    session
                        .summary
                        .as_ref()
                        .map(|_| "(含摘要)")
                        .unwrap_or("")
                )
            }
            _ => {
                if let Some(rest) = text.strip_prefix("/remember ") {
                    let content = rest.trim();
                    if content.is_empty() {
                        "用法:/remember <内容>".to_string()
                    } else {
                        session.memory.refresh();
                        let ok = session.memory.add(
                            content,
                            "user",
                            mem_cfg.max_entries as usize,
                            mem_cfg.max_entry_chars as usize,
                        );
                        if ok {
                            self.mark_memory_changed();
                        }
                        self.log(
                            "info",
                            &format!(
                                "[{}] 用户添加记忆{}: {content}",
                                session.key,
                                if ok { "" } else { "(重复或为空)" }
                            ),
                        );
                        if ok {
                            format!("🧠 已记住: {content}")
                        } else {
                            "这条记忆为空或已存在。".to_string()
                        }
                    }
                } else if let Some(rest) = text.strip_prefix("/forget ") {
                    let content = rest.trim();
                    session.memory.refresh();
                    // 仅支持英文逗号分隔的数字序号,如 /forget 1,3
                    let mut idxs: Vec<usize> = Vec::new();
                    let mut valid = true;
                    for part in content.split(',') {
                        let p = part.trim();
                        if p.is_empty() {
                            continue;
                        }
                        match p.parse::<usize>() {
                            Ok(i) => idxs.push(i),
                            Err(_) => {
                                valid = false;
                                break;
                            }
                        }
                    }
                    if !valid || idxs.is_empty() {
                        "用法:/forget <序号,逗号分隔,如 1,3>".to_string()
                    } else {
                        idxs.sort_unstable();
                        idxs.dedup();
                        // 从大到小删除,避免序号漂移
                        let mut removed = 0;
                        for idx in idxs.into_iter().rev() {
                            if session.memory.remove_index(idx) {
                                removed += 1;
                            }
                        }
                        if removed > 0 {
                            self.mark_memory_changed();
                        }
                        self.log(
                            "info",
                            &format!(
                                "[{}] 用户删除记忆: 序号 [{}],删除 {removed} 条",
                                session.key, content
                            ),
                        );
                        if removed > 0 {
                            format!("🗑️ 已删除 {removed} 条记忆。")
                        } else {
                            "未找到匹配的序号。".to_string()
                        }
                    }
                } else if text == "/memories" {
                    session.memory.refresh();
                    if session.memory.entries.is_empty() {
                        "🧠 暂无记忆。".to_string()
                    } else {
                        let scope = if msg.kind == MsgKind::Group {
                            "本群"
                        } else {
                            "本会话"
                        };
                        let mut s = format!(
                            "🧠 {scope}长期记忆({} 条):\n",
                            session.memory.entries.len()
                        );
                        for (i, e) in session.memory.entries.iter().enumerate() {
                            s.push_str(&format!(
                                "{}. [{} {}] {}\n",
                                i + 1,
                                if e.source == "model" { "自动" } else { "用户" },
                                crate::memory::fmt_ts(e.ts),
                                e.text
                            ));
                        }
                        s.push_str("\n💡 添加:/remember <内容> · 删除:/forget <序号,如 1,3>");
                        self.log(
                            "info",
                            &format!("[{}] 查看记忆: {} 条", session.key, session.memory.entries.len()),
                        );
                        s
                    }
                } else if text == "/model" {
                    format!("可用模型: {}", model_names.join(", "))
                } else if let Some(rest) = text.strip_prefix("/model ") {
                    let name = rest.trim();
                    if model_names.iter().any(|m| m == name) {
                        // 快照已 drop,此时可安全获取写锁(修复死锁)
                        {
                            let mut cfg = self.cfg.write().await;
                            cfg.active_model = name.to_string();
                        }
                        {
                            let cfg = self.cfg.read().await;
                            let _ = crate::config::save_config(&self.cfg_path, &cfg);
                        }
                        self.log("info", &format!("[{}] 切换模型 -> {name}", session.key));
                        format!("✅ 已切换模型: {name}")
                    } else {
                        format!("❌ 未找到模型 {name},可用: {}", model_names.join(", "))
                    }
                } else if text == "/prompt" {
                    format!("可用人设: {}", prompt_ids.join(", "))
                } else if let Some(rest) = text.strip_prefix("/prompt ") {
                    let id = rest.trim();
                    if prompt_ids.iter().any(|p| p == id) {
                        {
                            let mut cfg = self.cfg.write().await;
                            cfg.active_prompt = id.to_string();
                        }
                        {
                            let cfg = self.cfg.read().await;
                            let _ = crate::config::save_config(&self.cfg_path, &cfg);
                        }
                        self.log("info", &format!("[{}] 切换人设 -> {id}", session.key));
                        format!("✅ 已切换人设: {id}")
                    } else {
                        format!("❌ 未找到人设 {id},可用: {}", prompt_ids.join(", "))
                    }
                } else {
                    return None;
                }
            }
        };
        let _ = self.send_text(msg, &reply).await;
        Some(reply)
    }
}
