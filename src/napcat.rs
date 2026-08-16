//! NapCat 客户端:OneBot 11 协议。
//! 支持正向 WS(机器人主动连接 NapCat)与反向 WS(NapCat 连到本程序监听端口)。
//! 事件全量解析:message / notice(全部通知类型)/ request / meta_event,
//! 未知事件类型原样透传,保证「不丢任何通知」。
//!
//! 架构:
//! - 每次 WS 连接建立一个 ConnCtx(写通道 + echo 回执表),替换全局 ConnState.current;
//! - 业务层通过 ActionSender 发送动作,带 echo + oneshot 回执,超时 15s;
//! - 连接断开自动重连(正向指数退避),反向 WS 多连接取最新。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, RwLock, watch};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::{accept_hdr_async, connect_async, WebSocketStream};
use tokio_util::sync::CancellationToken;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;

use crate::config::Config;

// ---------- 事件类型(上行: WS -> 业务层) ----------

#[derive(Debug, Clone)]
pub enum BotEvent {
    /// 消息(群/私聊)
    Message(ParsedMsg),
    /// 通知(全部类型,含 NapCat 扩展)
    Notice(NoticeInfo),
    /// 请求(加好友/加群)
    Request(RequestInfo),
    /// 心跳(OneBot meta_event,带 NapCat 的连接状态:QQ 是否在线等)
    Heartbeat { online: bool, good: bool, interval_ms: u64 },
    /// 其他生命周期事件(未知类型原样描述)
    Lifecycle(String),
}

/// 解析后的消息(raw 等字段为扩展预留)
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ParsedMsg {
    pub kind: MsgKind,
    pub group_id: Option<i64>,
    pub user_id: i64,
    /// 是否机器人自己发出的
    pub is_self: bool,
    /// 纯文本(去除 CQ 码与富媒体段)
    pub text: String,
    /// 是否 at 了机器人
    pub at_me: bool,
    /// 是否回复(引用)了机器人
    pub reply_me: bool,
    /// 原始事件(供扩展使用)
    pub raw: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MsgKind {
    Group,
    Private,
}

/// 通知信息(全部 notice 类型统一结构)
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct NoticeInfo {
    pub notice_type: String,
    pub group_id: Option<i64>,
    pub user_id: Option<i64>,
    pub operator_id: Option<i64>,
    /// 人类可读描述(中文)
    pub desc: String,
    /// 原始事件
    pub raw: Value,
}

/// 请求信息(加好友/加群)
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RequestInfo {
    pub request_type: String,
    pub group_id: Option<i64>,
    pub user_id: i64,
    pub comment: String,
    pub flag: String,
    pub raw: Value,
}

// ---------- 动作发送(业务层 -> 当前连接写循环) ----------

pub type ActionResult = Result<Value, String>;

static NEXT_ECHO: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct ActionSender {
    state: Arc<ConnState>,
}

pub(crate) struct ConnState {
    pub(crate) current: RwLock<Option<ConnCtx>>,
    /// 当前连接任务(反向 WS 新连接替换时 abort 旧任务,防重复事件/任务泄漏)
    pub(crate) conn_task: RwLock<Option<tokio::task::JoinHandle<()>>>,
}

/// 一个活跃 WS 连接的上下文
#[derive(Clone)]
pub(crate) struct ConnCtx {
    gen: u64,
    writer: mpsc::Sender<WsOut>,
    echo_map: Arc<Mutex<HashMap<String, oneshot::Sender<ActionResult>>>>,
}

struct WsOut {
    payload: String,
}

impl ActionSender {
    pub async fn send(&self, action: &str, params: Value) -> Result<Value, String> {
        let ctx = self
            .state
            .current
            .read()
            .await
            .clone()
            .ok_or_else(|| "未连接到 NapCat".to_string())?;
        let echo = format!("lb_{}", NEXT_ECHO.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        ctx.echo_map.lock().unwrap().insert(echo.clone(), tx);
        let payload = json!({ "action": action, "params": params, "echo": echo });
        let out = WsOut {
            payload: serde_json::to_string(&payload).unwrap(),
        };
        // 统一出口:无论成败都清理 echo 表,避免泄漏
        let result: Result<Value, String> = if ctx.writer.send(out).await.is_err() {
            Err("连接已断开".to_string())
        } else {
            // 手动展开,避免 ? 提前返回跳过 echo 表清理
            match tokio::time::timeout(Duration::from_secs(15), rx).await {
                Ok(Ok(v)) => v,
                Ok(Err(_)) => Err("连接已断开".to_string()),
                Err(_) => Err("动作超时(15s)".to_string()),
            }
        };
        ctx.echo_map.lock().unwrap().remove(&echo);
        result
    }

    pub async fn send_group_msg(&self, group_id: i64, text: &str) -> Result<Value, String> {
        self.send(
            "send_group_msg",
            json!({ "group_id": group_id, "message": text }),
        )
        .await
    }

    pub async fn send_private_msg(&self, user_id: i64, text: &str) -> Result<Value, String> {
        self.send(
            "send_private_msg",
            json!({ "user_id": user_id, "message": text }),
        )
        .await
    }

    /// 查询登录信息(供外部/扩展使用)
    #[allow(dead_code)]
    pub async fn get_login_info(&self) -> Result<Value, String> {
        self.send("get_login_info", json!({})).await
    }
}

// ---------- 状态 ----------

#[derive(Serialize, Clone, Debug)]
pub struct ConnStatus {
    pub connected: bool,
    pub mode: String,
    pub endpoint: String,
    pub self_id: Option<i64>,
    pub last_error: String,
}

// ---------- 事件解析 ----------

fn parse_incoming(text: &str, self_id: Option<i64>) -> Option<BotEvent> {
    let v: Value = serde_json::from_str(text).ok()?;
    let post_type = v.get("post_type")?.as_str()?;
    match post_type {
        "message" => Some(BotEvent::Message(parse_message(&v, self_id))),
        // 自己发出的消息不进业务管线
        "message_sent" => None,
        "notice" => Some(BotEvent::Notice(parse_notice(&v))),
        "request" => Some(BotEvent::Request(parse_request(&v))),
        "meta_event" => {
            let st = v["meta_event_type"].as_str().unwrap_or("");
            if st == "heartbeat" {
                // NapCat 心跳带 status:{online,good}(QQ 登录态)与 interval(心跳间隔 ms)
                Some(BotEvent::Heartbeat {
                    online: v["status"]["online"].as_bool().unwrap_or(true),
                    good: v["status"]["good"].as_bool().unwrap_or(true),
                    interval_ms: v["interval"].as_u64().unwrap_or(0),
                })
            } else {
                Some(BotEvent::Lifecycle(st.to_string()))
            }
        }
        other => Some(BotEvent::Lifecycle(format!("未知事件类型: {other}"))),
    }
}

/// 解析消息事件(message 支持 CQ string 与 array 两种格式)
pub fn parse_message(v: &Value, self_id: Option<i64>) -> ParsedMsg {
    let kind = match v["message_type"].as_str() {
        Some("private") => MsgKind::Private,
        _ => MsgKind::Group,
    };
    let group_id = v["group_id"].as_i64();
    let user_id = v["user_id"].as_i64().unwrap_or(0);
    let msg = v.get("message").cloned().unwrap_or(Value::Null);
    let (text, at_me, reply_me) = extract_text(&msg, self_id);
    let is_self = self_id.map(|s| user_id == s).unwrap_or(false);
    ParsedMsg {
        kind,
        group_id,
        user_id,
        is_self,
        text,
        at_me,
        reply_me,
        raw: v.clone(),
    }
}

/// 提取纯文本 + at 检测 + 引用检测(兼容 array 与 CQ string 两种格式)
fn extract_text(msg: &Value, self_id: Option<i64>) -> (String, bool, bool) {
    let mut text = String::new();
    let mut at_me = false;
    let mut reply_me = false;
    match msg {
        Value::Array(segments) => {
            for seg in segments {
                let t = seg["type"].as_str().unwrap_or("");
                let data = &seg["data"];
                match t {
                    "text" => text.push_str(data["text"].as_str().unwrap_or("")),
                    "at" => {
                        let qq = data["qq"].as_str().unwrap_or("");
                        if qq == "all" {
                            // @全体:触发,并保留文本
                            at_me = true;
                            text.push_str("@全体成员 ");
                        } else if !qq.is_empty() {
                            // 修复:@ 特定成员时只在目标是机器人时触发;@ 别人保留目标文本
                            // (模型能分清"别人 @ 的是谁";@ 自己不加文本,避免破坏命令检测)
                            if self_id.map(|s| s.to_string() == qq).unwrap_or(false) {
                                at_me = true;
                            } else {
                                text.push_str(&format!("@QQ{qq} "));
                            }
                        }
                        // self_id 未知:特定 @ 一律不假定为 @ 机器人(修复误触发)
                    }
                    "reply" => {
                        // 修复:只有引用(回复)机器人自己的消息才触发;引用别人保留目标文本
                        let quoted = data["qq"].as_str().and_then(|q| q.parse::<i64>().ok());
                        match (quoted, self_id) {
                            (Some(q), Some(sid)) if q == sid => {
                                reply_me = true;
                                text.push_str("[引用回复] ");
                            }
                            (Some(q), _) => text.push_str(&format!("[引用QQ{q}] ")),
                            (None, _) => text.push_str("[引用] "),
                        }
                    }
                    "image" => text.push_str("[图片] "),
                    "face" => text.push_str("[表情] "),
                    "mface" => text.push_str("[商城表情] "),
                    "record" => text.push_str("[语音] "),
                    "video" => text.push_str("[视频] "),
                    "file" => text.push_str("[文件] "),
                    "json" => text.push_str("[卡片] "),
                    "xml" => text.push_str("[XML卡片] "),
                    "markdown" => text.push_str("[Markdown] "),
                    "miniapp" => text.push_str("[小程序] "),
                    "onlinefile" => text.push_str("[在线文件] "),
                    "flashtransfer" => text.push_str("[闪照] "),
                    "forward" => text.push_str("[聊天记录] "),
                    "node" => text.push_str("[合并转发节点] "),
                    "music" => text.push_str("[音乐] "),
                    "poke" => text.push_str("[戳一戳] "),
                    "dice" => text.push_str("[骰子] "),
                    "rps" => text.push_str("[猜拳] "),
                    "contact" => text.push_str("[名片] "),
                    "location" => text.push_str("[位置] "),
                    _ => {
                        if let Some(raw) = data.get("text") {
                            text.push_str(raw.as_str().unwrap_or(""));
                        }
                    }
                }
            }
        }
        Value::String(s) => {
            let mut rest = s.as_str();
            while let Some(start) = rest.find("[CQ:") {
                text.push_str(&rest[..start]);
                let (cq, after) = match rest[start..].find(']') {
                    Some(i) => (&rest[start..start + i], &rest[start + i + 1..]),
                    None => (&rest[start..], ""),
                };
                if let Some(args) = cq.strip_prefix("[CQ:") {
                    let (t, params) = args.split_once(',').unwrap_or((args, ""));
                    let get = |k: &str| {
                        params
                            .split(',')
                            .find_map(|kv| kv.strip_prefix(k))
                            .unwrap_or("")
                    };
                    match t {
                        "at" => {
                            let qq = get("qq=");
                            if qq == "all" {
                                at_me = true;
                                text.push_str("@全体成员 ");
                            } else if !qq.is_empty() {
                                if self_id.map(|s| s.to_string() == qq).unwrap_or(false) {
                                    at_me = true;
                                } else {
                                    text.push_str(&format!("@QQ{qq} "));
                                }
                            }
                        }
                        "reply" => {
                            let quoted = get("qq=").parse::<i64>().ok();
                            match (quoted, self_id) {
                                (Some(q), Some(sid)) if q == sid => {
                                    reply_me = true;
                                    text.push_str("[引用回复] ");
                                }
                                (Some(q), _) => text.push_str(&format!("[引用QQ{q}] ")),
                                (None, _) => text.push_str("[引用] "),
                            }
                        }
                        "image" => text.push_str("[图片] "),
                        "face" => text.push_str("[表情] "),
                        "record" => text.push_str("[语音] "),
                        "video" => text.push_str("[视频] "),
                        "file" => text.push_str("[文件] "),
                        "json" => text.push_str("[卡片] "),
                        _ => {}
                    }
                }
                rest = after;
            }
            text.push_str(rest);
        }
        _ => {}
    }
    (text.trim().to_string(), at_me, reply_me)
}

/// 全量解析通知(OneBot 11 全部 notice 类型 + NapCat 扩展,未知类型兜底)
pub fn parse_notice(v: &Value) -> NoticeInfo {
    let notice_type = v["notice_type"].as_str().unwrap_or("unknown").to_string();
    let group_id = v["group_id"].as_i64();
    let user_id = v["user_id"].as_i64();
    let operator_id = v["operator_id"].as_i64();
    let st = v["sub_type"].as_str().unwrap_or("");
    let desc = match notice_type.as_str() {
        "group_upload" => format!(
            "群文件上传: {} 上传了「{}」",
            uid(user_id),
            v["file"]["name"].as_str().unwrap_or("文件")
        ),
        "group_admin" => format!(
            "{} {}管理员",
            uid(user_id),
            if st == "set" { "成为" } else { "被取消" }
        ),
        "group_decrease" => {
            let kind = match st {
                "leave" => "主动退群",
                "kick" => "被移出群",
                "kick_me" => "机器人被移出群",
                _ => "成员减少",
            };
            format!("{} {}(操作者 {})", uid(user_id), kind, uid(operator_id))
        }
        "group_increase" => {
            let kind = if st == "invite" { "被邀请入群" } else { "入群" };
            format!("{} {}(操作者 {})", uid(user_id), kind, uid(operator_id))
        }
        "group_ban" => {
            if st == "lift_ban" {
                format!("{} 被解除禁言(操作者 {})", uid(user_id), uid(operator_id))
            } else {
                let dur = v["duration"].as_i64().unwrap_or(0);
                if dur == 0 {
                    format!("{} 被永久禁言(操作者 {})", uid(user_id), uid(operator_id))
                } else {
                    format!(
                        "{} 被禁言 {:.1} 分钟(操作者 {})",
                        uid(user_id),
                        dur as f64 / 60.0,
                        uid(operator_id)
                    )
                }
            }
        }
        "friend_add" => format!("新好友: {}", uid(user_id)),
        "group_recall" => format!(
            "{} 撤回了一条消息(操作者 {},消息ID {})",
            uid(user_id),
            uid(operator_id),
            v["message_id"].as_i64().unwrap_or(0)
        ),
        "friend_recall" => format!(
            "好友 {} 撤回了一条消息(消息ID {})",
            uid(user_id),
            v["message_id"].as_i64().unwrap_or(0)
        ),
        "notify" => match st {
            "poke" => match v["target_id"].as_i64() {
                Some(t) => format!("{} 戳了戳 {}", uid(user_id), uid(Some(t))),
                None => format!("{} 戳了戳某人", uid(user_id)),
            },
            "lucky_king" => format!("{} 成为红包运气王", uid(user_id)),
            "honor" => format!(
                "{} 获得群荣誉: {}",
                uid(user_id),
                v["honor_type"].as_str().unwrap_or("")
            ),
            "title" => format!("{} 获得群头衔", uid(user_id)),
            "essence" => format!("{} 添加了精华消息", uid(operator_id)),
            "group_sign" => format!("{} 完成群签到", uid(user_id)),
            "client_status" => format!("{} 客户端状态变化", uid(user_id)),
            other => format!("通知[notify/{other}]: {}", v),
        },
        // NapCat / OneBot 05 常见扩展
        "essence" => format!("精华消息变更(操作者 {})", uid(operator_id)),
        "group_sign" => format!("{} 完成群签到", uid(user_id)),
        "client_status" => format!("客户端状态变化: {}", v),
        "group_card" => format!(
            "{} 的群名片变为「{}」",
            uid(user_id),
            v["card_new"].as_str().unwrap_or("")
        ),
        "group_title" => format!(
            "{} 的群头衔变为「{}」",
            uid(user_id),
            v["title_new"].as_str().unwrap_or("")
        ),
        "group_lucky_king" => format!("{} 成为红包运气王", uid(user_id)),
        "group_receipt" => format!("群收款事件: {}", v),
        "group_essence" => format!("{} 添加了精华消息", uid(operator_id)),
        "group_poke" => format!("{} 戳了戳 {}", uid(user_id), uid(v["target_id"].as_i64())),
        "group_honor" => format!(
            "{} 获得群荣誉: {}",
            uid(user_id),
            v["honor_type"].as_str().unwrap_or("")
        ),
        _ => format!("通知[{}]: {}", notice_type, v),
    };
    NoticeInfo {
        notice_type,
        group_id,
        user_id,
        operator_id,
        desc,
        raw: v.clone(),
    }
}

fn parse_request(v: &Value) -> RequestInfo {
    RequestInfo {
        request_type: v["request_type"].as_str().unwrap_or("unknown").to_string(),
        group_id: v["group_id"].as_i64(),
        user_id: v["user_id"].as_i64().unwrap_or(0),
        comment: v["comment"].as_str().unwrap_or("").to_string(),
        flag: v["flag"].as_str().unwrap_or("").to_string(),
        raw: v.clone(),
    }
}

fn uid(id: Option<i64>) -> String {
    match id {
        Some(i) => i.to_string(),
        None => "未知".to_string(),
    }
}

// ---------- 客户端主体 ----------

pub struct NapcatClient {
    pub cfg: Arc<RwLock<Config>>,
    /// 连接模式(构造时快照,供同步状态更新使用)
    mode: String,
    /// 事件上行通道(给业务层)
    pub events: mpsc::Sender<BotEvent>,
    /// 连接状态广播
    pub status: watch::Sender<ConnStatus>,
    /// 当前连接(动作发送用)
    pub(crate) conn: Arc<ConnState>,
    /// 机器人 QQ(连接后自动获取,供 at 检测)
    pub self_id: Arc<AtomicI64>,
}

static NEXT_GEN: AtomicU64 = AtomicU64::new(1);

impl NapcatClient {
    pub async fn new(
        cfg: Arc<RwLock<Config>>,
        events: mpsc::Sender<BotEvent>,
        status: watch::Sender<ConnStatus>,
    ) -> Self {
        let (mode, endpoint) = {
            let c = cfg.read().await;
            let mode = c.napcat.mode.clone();
            let endpoint = if mode == "reverse" {
                format!("0.0.0.0:{}", c.napcat.reverse_port)
            } else {
                c.napcat.ws_url.clone()
            };
            (mode, endpoint)
        };
        let _ = status.send(ConnStatus {
            connected: false,
            mode: mode.clone(),
            endpoint,
            self_id: None,
            last_error: String::new(),
        });
        let sid = cfg.read().await.napcat.self_id.trim().parse().ok();
        Self {
            cfg,
            mode,
            events,
            status,
            conn: Arc::new(ConnState {
                current: RwLock::new(None),
                conn_task: RwLock::new(None),
            }),
            self_id: Arc::new(AtomicI64::new(sid.unwrap_or(0))),
        }
    }

    /// 按配置模式启动连接循环(直到 cancel 触发),返回动作发送器与连接任务句柄
    pub async fn run(
        self: Arc<Self>,
        cancel: CancellationToken,
    ) -> (ActionSender, tokio::task::JoinHandle<()>) {
        let sender = ActionSender {
            state: self.conn.clone(),
        };
        let cfg = self.cfg.read().await;
        let handle = if cfg.napcat.mode == "reverse" {
            let me = self.clone();
            tokio::spawn(me.run_reverse(cancel))
        } else {
            let me = self.clone();
            tokio::spawn(me.run_forward(cancel))
        };
        (sender, handle)
    }

    async fn run_forward(self: Arc<Self>, cancel: CancellationToken) {
        let mut backoff: u64 = 1;
        loop {
            let connected = tokio::select! {
                _ = cancel.cancelled() => break,
                ok = self.clone().connect_once_forward(backoff) => ok,
            };
            // 修复:连接成功后重置退避(历史 bug:长时间在线后断线也要等满 30s);
            // 成功连接后强制小睡 2s,避免"连上即断"时热循环打爆 NapCat
            backoff = if connected {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
                1
            } else {
                (backoff * 2).min(30)
            };
        }
    }

    /// 单轮正向连接:连接 -> 运行 -> 断开 -> 等待退避;返回是否成功建立过连接
    async fn connect_once_forward(self: Arc<Self>, backoff: u64) -> bool {
        let url = {
            let cfg = self.cfg.read().await;
            build_ws_url(&cfg)
        };
        self.update_status(false, &url, None, "");
        let conn = connect_async(&url).await;
        match conn {
            Ok((ws, _resp)) => {
                self.update_status(true, &url, None, "");
                let _ = self.clone().run_connection(ws).await;
                self.update_status(false, &url, None, "连接已断开");
                true
            }
            Err(e) => {
                self.update_status(false, &url, None, &format!("连接失败: {e}"));
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                false
            }
        }
    }

    async fn run_reverse(self: Arc<Self>, cancel: CancellationToken) {
        let (port, token) = {
            let cfg = self.cfg.read().await;
            (cfg.napcat.reverse_port, cfg.napcat.access_token.clone())
        };
        // 端口配置非法时上报错误并退出监听任务(而非 panic 拖垮整个运行时)
        let Ok(addr): Result<SocketAddr, _> = format!("0.0.0.0:{port}").parse() else {
            self.update_status(
                false,
                &format!("0.0.0.0:{port}"),
                None,
                &format!("反向 WS 端口配置无效: {port}(请检查设置)"),
            );
            return;
        };
        let listener = match TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                self.update_status(false, &addr.to_string(), None, &format!("监听失败: {e}"));
                return;
            }
        };
        let endpoint = addr.to_string();
        self.update_status(false, &endpoint, None, "等待 NapCat 连接…");
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                r = listener.accept() => {
                    let (stream, _) = match r {
                        Ok(v) => v,
                        Err(e) => {
                            self.update_status(false, &endpoint, None, &format!("accept 失败: {e}"));
                            continue;
                        }
                    };
                    // 新连接替换旧连接:先停掉旧连接任务(防重复事件转发/任务泄漏)
                    let old = self.conn.conn_task.write().await.take();
                    if let Some(h) = old {
                        h.abort();
                    }
                    let me = self.clone();
                    let token = token.clone();
                    let endpoint = endpoint.clone();
                    let handle = tokio::spawn(async move {
                        match me.handshake(stream, &token).await {
                            Ok(ws) => {
                                me.update_status(true, &endpoint, None, "");
                                me.clone().run_connection(ws).await;
                                me.update_status(false, &endpoint, None, "NapCat 连接已断开");
                            }
                            Err(e) => {
                                me.update_status(false, &endpoint, None, &e);
                            }
                        }
                    });
                    *self.conn.conn_task.write().await = Some(handle);
                }
            }
        }
    }

    /// WS 握手 + access_token 校验(Authorization: Bearer xxx)
    async fn handshake(
        &self,
        stream: TcpStream,
        token: &str,
    ) -> Result<WebSocketStream<TcpStream>, String> {
        let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
            if !token.is_empty() {
                let auth = req
                    .headers()
                    .get("Authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if auth != format!("Bearer {token}") {
                    return Err(http::Response::builder()
                        .status(http::StatusCode::UNAUTHORIZED)
                        .body(None::<String>)
                        .unwrap());
                }
            }
            Ok(resp)
        };
        accept_hdr_async(stream, callback)
            .await
            .map_err(|e| format!("握手失败: {e}"))
    }

    /// 已建立的连接:建立 ConnCtx -> 读写双循环 -> 退出时清理
    async fn run_connection<S>(self: Arc<Self>, ws: WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let gen = NEXT_GEN.fetch_add(1, Ordering::Relaxed);
        let (writer_tx, writer_rx) = mpsc::channel::<WsOut>(128);
        let ctx = ConnCtx {
            gen,
            writer: writer_tx,
            echo_map: Arc::new(Mutex::new(HashMap::new())),
        };
        // 替换当前连接
        {
            let mut cur = self.conn.current.write().await;
            *cur = Some(ctx.clone());
        }

        let (mut w, mut r) = ws.split();
        let (ev_tx, mut ev_rx) = mpsc::channel::<BotEvent>(256);
        let events = self.events.clone();
        let ctx_r = ctx.clone();
        let self_id = self.self_id.clone();
        let status = self.status.clone();

        // 读循环:事件上行 + echo 回执
        let reader = tokio::spawn(async move {
            while let Some(Ok(msg)) = r.next().await {
                match msg {
                    WsMessage::Text(t) => {
                        let s: String = t.to_string();
                        let v: Value = match serde_json::from_str(&s) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let Some(echo) = v.get("echo").and_then(|e| e.as_str()) {
                            if echo == "lb_self" {
                                // 登录信息回执:自动填充机器人 QQ(保留其他状态字段)
                                if let Some(uid) = v["data"]["user_id"].as_i64() {
                                    self_id.store(uid, Ordering::Relaxed);
                                    let cur = status.borrow().clone();
                                    let _ = status.send(ConnStatus {
                                        self_id: Some(uid),
                                        ..cur
                                    });
                                }
                                continue;
                            }
                            if let Some(tx) = ctx_r.echo_map.lock().unwrap().remove(echo) {
                                let ok = v.get("status").and_then(|s| s.as_str()) == Some("ok");
                                let _ = tx.send(if ok {
                                    Ok(v.get("data").cloned().unwrap_or(Value::Null))
                                } else {
                                    Err(v["message"]
                                        .as_str()
                                        .unwrap_or("动作执行失败")
                                        .to_string())
                                });
                            }
                            continue;
                        }
                        let sid = self_id.load(Ordering::Relaxed);
                        let sid = (sid > 0).then_some(sid);
                        if let Some(ev) = parse_incoming(&s, sid) {
                            let _ = ev_tx.send(ev).await;
                        }
                    }
                    WsMessage::Close(_) => break,
                    WsMessage::Ping(_) => {
                        // ping/pong 由底层协议栈处理,忽略即可;
                        // OneBot 心跳是应用层 meta_event,由 parse_incoming 处理
                    }
                    _ => {}
                }
            }
        });

        // 事件转发:连接读循环 -> 全局事件总线
        let forwarder = tokio::spawn(async move {
            while let Some(ev) = ev_rx.recv().await {
                if events.send(ev).await.is_err() {
                    break;
                }
            }
        });

        // 写循环:消费动作
        let writer_task = tokio::spawn(async move {
            let mut rx = writer_rx;
            while let Some(out) = rx.recv().await {
                if w.send(WsMessage::Text(Utf8Bytes::from(out.payload))).await.is_err() {
                    break;
                }
            }
        });

        // 连接建立后请求一次登录信息(用于 at 检测;配置了 self_id 则跳过)
        if self.self_id.load(Ordering::Relaxed) <= 0 {
            let payload = json!({ "action": "get_login_info", "params": {}, "echo": "lb_self" });
            let _ = ctx
                .writer
                .send(WsOut {
                    payload: serde_json::to_string(&payload).unwrap(),
                })
                .await;
        }

        let _ = tokio::select! {
            _ = reader => {}
            _ = writer_task => {}
            _ = forwarder => {}
        };

        // 清理:仅当自己仍是当前连接时清空
        {
            let mut cur = self.conn.current.write().await;
            if let Some(c) = cur.as_ref() {
                if c.gen == gen {
                    *cur = None;
                }
            }
        }
    }

    fn update_status(&self, connected: bool, endpoint: &str, self_id: Option<i64>, err: &str) {
        let _ = self.status.send(ConnStatus {
            connected,
            mode: self.mode.clone(),
            endpoint: endpoint.to_string(),
            self_id: self_id.or_else(|| {
                let s = self.self_id.load(Ordering::Relaxed);
                (s > 0).then_some(s)
            }),
            last_error: err.to_string(),
        });
    }
}

fn build_ws_url(cfg: &Config) -> String {
    let mut url = cfg.napcat.ws_url.trim().to_string();
    if !cfg.napcat.access_token.trim().is_empty() {
        let sep = if url.contains('?') { '&' } else { '?' };
        url.push(sep);
        url.push_str(&format!("access_token={}", cfg.napcat.access_token.trim()));
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_array_msg() {
        let v = json!({
            "message_type": "group",
            "group_id": 123,
            "user_id": 456,
            "message": [
                {"type": "at", "data": {"qq": "789"}},
                {"type": "text", "data": {"text": " 你好"}},
                {"type": "image", "data": {"file": "x.png"}}
            ]
        });
        let m = parse_message(&v, Some(789));
        assert!(m.at_me);
        assert_eq!(m.text, "你好[图片]");
        assert!(!m.is_self);
    }

    #[test]
    fn parse_cq_msg() {
        let v = json!({
            "message_type": "private",
            "user_id": 456,
            "message": "[CQ:at,qq=789]hello[CQ:face,id=1]"
        });
        let m = parse_message(&v, Some(789));
        assert!(m.at_me);
        assert_eq!(m.text, "hello[表情]");
    }

    #[test]
    fn at_others_and_reply_targets() {
        // @ 别人:不触发,保留 @ 目标文本(模型能分清 @ 的是谁)
        let v = json!({
            "message_type": "group", "group_id": 1, "user_id": 2,
            "message": [
                {"type": "at", "data": {"qq": "999"}},
                {"type": "text", "data": {"text": "在吗"}}
            ]
        });
        let m = parse_message(&v, Some(789));
        assert!(!m.at_me);
        assert_eq!(m.text, "@QQ999 在吗");

        // self_id 未知:@ 特定成员不再假定触发(修复误触发);@全体仍触发
        assert!(!parse_message(&v, None).at_me);
        let v_all = json!({
            "message_type": "group", "group_id": 1, "user_id": 2,
            "message": [{"type": "at", "data": {"qq": "all"}}, {"type": "text", "data": {"text": "hi"}}]
        });
        assert!(parse_message(&v_all, None).at_me);

        // 引用机器人自己的消息:触发
        let vq1 = json!({
            "message_type": "group", "group_id": 1, "user_id": 2,
            "message": [{"type": "reply", "data": {"id": "1", "qq": "789"}}, {"type": "text", "data": {"text": "收到"}}]
        });
        let mq = parse_message(&vq1, Some(789));
        assert!(mq.reply_me);
        assert_eq!(mq.text, "[引用回复] 收到");

        // 引用别人的消息:不触发,保留引用目标(修复:此前任何引用都算引用机器人)
        let vq2 = json!({
            "message_type": "group", "group_id": 1, "user_id": 2,
            "message": [{"type": "reply", "data": {"id": "1", "qq": "999"}}, {"type": "text", "data": {"text": "收到"}}]
        });
        let mq2 = parse_message(&vq2, Some(789));
        assert!(!mq2.reply_me);
        assert_eq!(mq2.text, "[引用QQ999] 收到");

        // 引用数据缺少 qq:不假定触发
        let vq3 = json!({
            "message_type": "group", "group_id": 1, "user_id": 2,
            "message": [{"type": "reply", "data": {"id": "1"}}, {"type": "text", "data": {"text": "hi"}}]
        });
        assert!(!parse_message(&vq3, Some(789)).reply_me);

        // CQ 字符串:引用别人
        let vc = json!({
            "message_type": "group", "group_id": 1, "user_id": 2,
            "message": "[CQ:reply,id=1,qq=999]hello"
        });
        let mc = parse_message(&vc, Some(789));
        assert!(!mc.reply_me);
        assert_eq!(mc.text, "[引用QQ999] hello");
    }

    #[test]
    fn parse_notice_all_types() {
        let cases = [
            ("group_increase", json!({"post_type":"notice","notice_type":"group_increase","group_id":1,"user_id":2,"operator_id":3,"sub_type":"invite"})),
            ("group_ban", json!({"post_type":"notice","notice_type":"group_ban","group_id":1,"user_id":2,"operator_id":3,"duration":600})),
            ("notify_poke", json!({"post_type":"notice","notice_type":"notify","sub_type":"poke","group_id":1,"user_id":2,"target_id":4})),            ("group_upload", json!({"post_type":"notice","notice_type":"group_upload","group_id":1,"user_id":2,"file":{"name":"a.zip"}})),
            ("xxx_new_thing", json!({"post_type":"notice","notice_type":"xxx_new_thing","group_id":1})),
        ];
        for (t, v) in &cases {
            let n = parse_notice(v);
            if *t == "notify_poke" {
                assert_eq!(n.notice_type, "notify");
                assert!(n.desc.contains("戳了戳"), "desc: {}", n.desc);
            } else {
                assert_eq!(n.notice_type, *t);
            }
            assert!(!n.desc.is_empty());
        }
        // 未知类型不丢事件
        let n = parse_notice(&cases[4].1);
        assert!(n.desc.starts_with("通知[xxx_new_thing]"));
    }
}
