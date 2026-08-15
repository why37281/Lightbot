mod chat;
mod commands;
mod config;
mod cost;
mod llm;
mod memory;
mod napcat;
mod placement;
mod trace;
mod trigger;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            commands::setup(app)?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::get_config,
            commands::save_config,
            commands::get_config_path,
            commands::get_events,
            commands::start_bot,
            commands::stop_bot,
            commands::set_paused,
            commands::test_napcat,
            commands::test_llm,
            commands::get_sessions,
            commands::clear_session,
            commands::clear_trace,
            commands::get_session_detail,
            commands::update_history_msg,
            commands::delete_history_msg,
            commands::stop_session,
            commands::get_status_view,
            commands::get_all_memories,
            commands::add_memory,
            commands::delete_memory,
            commands::get_cost_overview,
            commands::query_balance,
            commands::get_placement_proposal,
            commands::approve_placement,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

// ---------- 端到端集成测试 ----------
// 模拟 NapCat 通过反向 WS 连接,发送群消息事件,
// 验证:握手 -> 事件解析 -> 触发判断 -> 会话创建 -> LLM 调用(无 key 必然失败)
//       -> 失败回复发送 -> 动作回执,全链路。
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures_util::SinkExt;
    use serde_json::json;
    use tokio::sync::{mpsc, watch, RwLock};
    use tokio_tungstenite::connect_async;
    use tokio_util::sync::CancellationToken;

    use crate::chat::ChatCore;
    use crate::config::Config;
    use crate::napcat::{BotEvent, ConnStatus, NapcatClient};

    #[tokio::test]
    async fn e2e_reverse_ws_full_pipeline() {
        let port = 18765u16;
        let mut cfg = Config::default();
        cfg.napcat.mode = "reverse".into();
        cfg.napcat.reverse_port = port;
        cfg.napcat.reply_pending = false;
        cfg.napcat.group_trigger = "at".into();
        cfg.napcat.self_id = "10001".into();
        cfg.models[0].api_key = String::new(); // 无 key:LLM 调用必然失败,验证失败路径
        cfg.models[0].timeout_secs = 10;
        cfg.chat.context_tokens = 1024;

        let cfg = Arc::new(RwLock::new(cfg));
        let (ev_tx, mut ev_rx) = mpsc::channel::<BotEvent>(64);
        let (status_tx, _status_rx) = watch::channel(ConnStatus {
            connected: false,
            mode: "reverse".into(),
            endpoint: String::new(),
            self_id: None,
            last_error: String::new(),
        });
        let cancel = CancellationToken::new();

        let napcat = Arc::new(NapcatClient::new(cfg.clone(), ev_tx, status_tx).await);
        let (sender, _conn_task) = napcat.clone().run(cancel.clone()).await;

        let events = Arc::new(std::sync::Mutex::new(crate::chat::EventBuf::new()));
        let cost = Arc::new(std::sync::Mutex::new(
            crate::cost::CostTracker::new(std::env::temp_dir().join("lightbot_e2e_usage")),
        ));
        let placement = Arc::new(std::sync::Mutex::new(
            crate::placement::PlacementController::default(),
        ));
        let paused = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (decide_cancel, _) = watch::channel::<bool>(false);
        let chat = Arc::new(ChatCore::new(
            cfg.clone(),
            sender,
            std::env::temp_dir().join("lightbot_e2e_sessions"),
            std::env::temp_dir().join("lightbot_e2e_config.json"),
            events,
            cost,
            placement,
            paused,
            decide_cancel,
        ));

        // 事件管线
        let chat2 = chat.clone();
        let pipeline = tokio::spawn(async move {
            while let Some(ev) = ev_rx.recv().await {
                if let BotEvent::Message(m) = ev {
                    chat2.handle_message(m).await;
                }
            }
        });

        // 等监听端口就绪
        tokio::time::sleep(Duration::from_millis(300)).await;

        // 模拟 NapCat:反向连接 + 发送群消息
        let url = format!("ws://127.0.0.1:{port}");
        let (ws, _) = connect_async(&url)
            .await
            .expect("反向 WS 连接失败(监听未就绪?)");
        let ws = Arc::new(tokio::sync::Mutex::new(ws));

        // 收动作 + 回执(独占 ws)
        let ws2 = ws.clone();
        let reader = tokio::spawn(async move {
            let mut got_send = 0usize;
            let mut got_reply = 0usize;
            for _ in 0..60 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let mut ws = ws2.lock().await;
                let msg = tokio::time::timeout(
                    Duration::from_millis(50),
                    futures_util::StreamExt::next(&mut *ws),
                )
                .await;
                let Ok(Some(msg)) = msg else {
                    continue;
                };
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                if let tokio_tungstenite::tungstenite::Message::Text(t) = msg {
                    let s: String = t.to_string();
                    let v: serde_json::Value =
                        serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);
                    // 动作(带 action 字段且无 status);回执由 status 字段识别
                    if v.get("action").is_some() && v.get("status").is_none() {
                        let action = v["action"].as_str().unwrap_or("");
                        if action == "send_group_msg" {
                            got_send += 1;
                            let text = v["params"]["message"].as_str().unwrap_or("");
                            if text.contains("出错了") {
                                got_reply += 1;
                            }
                        }
                        // 回复回执
                        let echo = v["echo"].as_str().unwrap_or("");
                        let ack = json!({
                            "status": "ok",
                            "retcode": 0,
                            "data": { "message_id": 1 },
                            "echo": echo
                        });
                        let _ = ws
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                tokio_tungstenite::tungstenite::Utf8Bytes::from(
                                    serde_json::to_string(&ack).unwrap(),
                                ),
                            ))
                            .await;
                    }
                    if v.get("echo").is_some() && v["echo"] == "lb_self" {
                        let ack = json!({
                            "status": "ok",
                            "data": { "user_id": 10001 },
                            "echo": "lb_self"
                        });
                        let _ = ws
                            .send(tokio_tungstenite::tungstenite::Message::Text(
                                tokio_tungstenite::tungstenite::Utf8Bytes::from(
                                    serde_json::to_string(&ack).unwrap(),
                                ),
                            ))
                            .await;
                    }
                }
            }
            (got_send, got_reply)
        });

        // 发送群消息 1:at 机器人触发
        tokio::time::sleep(Duration::from_millis(200)).await;
        let msg = json!({
            "post_type": "message",
            "message_type": "group",
            "group_id": 12345,
            "user_id": 67890,
            "self_id": 10001,
            "message": [
                {"type": "at", "data": {"qq": "10001"}},
                {"type": "text", "data": {"text": "你好,介绍一下自己"}}
            ]
        });
        {
            let mut ws = ws.lock().await;
            let _ = ws
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    tokio_tungstenite::tungstenite::Utf8Bytes::from(
                        serde_json::to_string(&msg).unwrap(),
                    ),
                ))
                .await;
        }

        // 发送群消息 2:软 at(提到称呼,不 at)→ 应触发完整通道
        tokio::time::sleep(Duration::from_millis(400)).await;
        let msg2 = json!({
            "post_type": "message",
            "message_type": "group",
            "group_id": 12345,
            "user_id": 67890,
            "self_id": 10001,
            "message": [
                {"type": "text", "data": {"text": "小灯在吗"}}
            ]
        });
        {
            let mut ws = ws.lock().await;
            let _ = ws
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    tokio_tungstenite::tungstenite::Utf8Bytes::from(
                        serde_json::to_string(&msg2).unwrap(),
                    ),
                ))
                .await;
        }

        // 发送群消息 3:斜杠命令(无 key 也能处理,不走 LLM;回归:命令死锁 bug)
        tokio::time::sleep(Duration::from_millis(400)).await;
        let msg3 = json!({
            "post_type": "message",
            "message_type": "group",
            "group_id": 12345,
            "user_id": 67890,
            "self_id": 10001,
            "message": [
                {"type": "text", "data": {"text": "/stats"}}
            ]
        });
        {
            let mut ws = ws.lock().await;
            let _ = ws
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    tokio_tungstenite::tungstenite::Utf8Bytes::from(
                        serde_json::to_string(&msg3).unwrap(),
                    ),
                ))
                .await;
        }

        let (got_send, got_reply) = tokio::time::timeout(Duration::from_secs(20), reader)
            .await
            .expect("测试超时")
            .expect("reader 任务失败");

        pipeline.abort();
        cancel.cancel();
        // 清理会话文件
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("lightbot_e2e_sessions"));
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("lightbot_e2e_usage"));

        assert!(got_send >= 3, "应收到至少 3 次 send_group_msg(at 触发 + 软 at 触发 + /stats 命令),实际 {got_send}");
        assert!(
            got_reply >= 2,
            "无 key 时 LLM 调用应失败并回复「出错了」提示,实际 {got_reply}"
        );
    }
}
