//! SubAgent 子系统核心。
//!
//! 整体流程:
//! 1. 主模型在回复末尾输出 `[任务:QQ 目标...]` / `[任务:沙箱 目标...]` 标记,
//!    chat.rs 剥离标记并调用 [`AgentManager::spawn`];
//! 2. 创建任务:校验模式开关与触发者权限(群主/管理员/白名单);
//! 3. 沙箱模式先「搭建 → 激活」(jail 建目录 / docker 拉镜像),QQ 操作模式无需沙箱;
//! 4. 阶段一「工具选择」:子上下文 = 固定系统提示 + 工具目录(名称+一行简述)+ 任务描述,
//!    模型输出 JSON `{"tools":[...], "plan":...}`(提示要求尽量一次选全);
//! 5. 阶段二「执行」:子上下文 = 固定系统提示 + 所选工具详细用法 + 任务 + 轮次历史,
//!    每轮输出 `{"tool","params","note"}` 或 `{"done":true,"summary"}`;
//!    - **每一步工具调用都进入审批队列,GUI 面板必须由用户亲自批准/拒绝后才执行**;
//!    - 暂停:冻结任务,主对话继续;恢复后续跑;停止:终止任务并清理沙箱;
//! 6. 完成后把简短摘要交给 chat.rs(注入主模型上下文尾部 + 可选触发主模型主动汇报);
//! 7. 子上下文随任务丢弃、从不持久化 → 天然「重置到初始状态(仅工具目录)」;
//!    两种模式之间、与主会话之间都不共享上下文。
//!
//! 成本设计:阶段一/阶段二各自的前缀(系统提示+目录/用法)固定,任务间与轮次间缓存命中;
//! 子调用关闭思考模式(结构化 JSON 输出,不需要推理链),输出预算由配置控制。

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::{oneshot, Mutex, RwLock, watch};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::cost::{CostTracker, UsageRecord};
use crate::events::{EventBuf, FrontendEvent};
use crate::llm::{ApiMessage, LlmClient};
use crate::napcat::{ActionSender, MsgKind};
use crate::sandbox::{self, SandboxBackend};
use crate::trace;

/// 主模型引导(恒定注入前缀区,缓存友好;⚠️ 文本改动会破坏缓存前缀,勿随意修改)。
/// 模式的启停状态绝不改这句,而是以「上下文尾部追加显式说明」体现(见 context.rs)。
pub const AGENT_GUIDE: &str = "(你可以把需要实际操作的事交给子助手:在回复末尾用标记 [任务:QQ] 后跟一句自然语言描述(群操作,如设置群精华、改好友备注、群打卡、发送图片文件);或用 [任务:沙箱] 后跟自然语言描述(读取/写入文件、执行命令、下载或发送 QQ 上的文件图片)。子助手完成后会返回简短摘要,由你向群友汇报。仅当用户启用了对应模式时才使用,不要为琐事使用。)";

/// 阶段一系统规则(固定前缀,缓存友好)
const SELECT_RULES: &str = "\
你是子助手「调度者」。根据用户的目标,从下面的工具目录中选取完成目标需要的工具。\n\
规则:\n\
- 只输出一个 JSON 对象,不要输出任何其他文字、代码块标记或解释;\n\
- 格式: {\"tools\": [\"工具名1\", \"工具名2\"], \"plan\": \"一句话执行计划\"}\n\
- 尽量一次选全完成目标需要的全部工具(可以多选);宁可多选一两个查询类工具,也不要漏选;\n\
- 如果目标无法用这些工具完成,输出: {\"tools\": [], \"plan\": \"无法完成的原因\"}\n\
- 只选目录里列出的工具名,不要编造。";

/// 阶段二系统规则(固定前缀,缓存友好)
const EXEC_RULES: &str = "\
你是子助手「执行者」。你负责执行用户交代的任务,使用下面提供的工具。\n\
规则:\n\
- 每轮只输出一个 JSON 对象,不要输出任何其他文字、代码块标记或解释;\n\
- 需要执行工具时输出: {\"tool\": \"工具名\", \"params\": {参数}, \"note\": \"这一步要做什么(简短)\"}\n\
- 任务完成时输出: {\"done\": true, \"summary\": \"完成摘要(简短,给主模型看)\"}\n\
- 不要编造工具执行结果;每一步必须等工具结果返回后再决定下一步;\n\
- 某步被用户拒绝时,换一种方式继续,或直接输出 done 结束;\n\
- 尽量用最少的轮次完成;信息不足时用查询类工具获取,不要猜测。";

// ---------- 工具定义 ----------

pub struct ToolDef {
    pub name: &'static str,
    /// 阶段一目录:一行简述(简短)
    pub brief: &'static str,
    /// 阶段二用法:详细参数说明
    pub usage: &'static str,
    /// 敏感工具:GUI 红字警示 + 仅群主/管理员(或白名单)任务可用
    pub sensitive: bool,
}

/// QQ 操作模式工具目录(动作名与 NapCat OpenAPI 一致)
const QQ_TOOLS: &[ToolDef] = &[
    ToolDef { name: "send_group_msg", brief: "向指定群发送消息(文本或图片)", usage: "参数: group_id(群号数字), message(文本字符串,或消息段数组,如图片 [{\"type\":\"image\",\"data\":{\"file\":\"本地路径或URL\"}}])", sensitive: false },
    ToolDef { name: "send_private_msg", brief: "向指定用户发送私聊消息(文本或图片)", usage: "参数: user_id(QQ号), message(同 send_group_msg)", sensitive: false },
    ToolDef { name: "set_essence_msg", brief: "把某条消息设置为群精华", usage: "参数: message_id(消息ID,用 get_msg 或群历史查询获取)", sensitive: false },
    ToolDef { name: "set_friend_remark", brief: "修改好友备注", usage: "参数: user_id(QQ号), remark(新备注文本)", sensitive: false },
    ToolDef { name: "set_group_card", brief: "修改群成员群名片", usage: "参数: group_id(群号), user_id(成员QQ), card(新名片,空字符串=清除)", sensitive: false },
    ToolDef { name: "send_group_sign", brief: "群打卡(每日签到)", usage: "参数: group_id(群号)", sensitive: false },
    ToolDef { name: "get_msg", brief: "按消息ID获取消息内容", usage: "参数: message_id(消息ID)", sensitive: false },
    ToolDef { name: "get_group_msg_history", brief: "获取群最近消息", usage: "参数: group_id(群号), count(数量,1~50)", sensitive: false },
    ToolDef { name: "get_group_member_info", brief: "获取群成员信息(角色等)", usage: "参数: group_id(群号), user_id(QQ号)", sensitive: false },
    ToolDef { name: "send_poke", brief: "戳一戳(群内或好友)", usage: "参数: user_id(QQ号), group_id(群号,群内戳可填)", sensitive: false },
    ToolDef { name: "upload_group_file", brief: "上传本地文件到群", usage: "参数: group_id(群号), file(本地绝对路径), name(显示文件名)", sensitive: false },
    ToolDef { name: "get_file", brief: "获取QQ文件/图片的本地路径", usage: "参数: file_id(消息里的文件ID)", sensitive: false },
    ToolDef { name: "delete_msg", brief: "撤回一条消息", usage: "参数: message_id(消息ID)", sensitive: true },
    ToolDef { name: "set_group_ban", brief: "禁言群成员", usage: "参数: group_id(群号), user_id(QQ号), duration(秒,0=解除)", sensitive: true },
    ToolDef { name: "set_group_whole_ban", brief: "全体禁言/解除", usage: "参数: group_id(群号), enable(true=禁言,false=解除)", sensitive: true },
    ToolDef { name: "set_group_kick", brief: "把成员移出群", usage: "参数: group_id(群号), user_id(QQ号), reject_add_request(可选,true=同时拉黑)", sensitive: true },
];

/// 沙箱模式工具目录
const SANDBOX_TOOLS: &[ToolDef] = &[
    ToolDef { name: "sandbox_read_file", brief: "读取沙箱内文件内容", usage: "参数: path(相对沙箱根的路径,如 work/a.txt)", sensitive: false },
    ToolDef { name: "sandbox_write_file", brief: "写入文件到沙箱", usage: "参数: path(相对沙箱根), content(内容)", sensitive: false },
    ToolDef { name: "sandbox_list_dir", brief: "列出沙箱内目录", usage: "参数: path(可选,相对沙箱根,默认根目录)", sensitive: false },
    ToolDef { name: "sandbox_run_cmd", brief: "在沙箱内执行命令", usage: "参数: cmd(可执行文件名,须在白名单), args(参数数组,可选);工作目录为沙箱 work/", sensitive: false },
    ToolDef { name: "sandbox_download_qq_file", brief: "把QQ上的文件/图片复制进沙箱", usage: "参数: file_id(QQ消息里的文件/图片ID)", sensitive: false },
    ToolDef { name: "sandbox_send_file", brief: "把沙箱内文件发送到QQ", usage: "参数: target_type(\"group\"或\"private\"), target_id(群号或QQ号), path(相对沙箱根), name(可选显示名)", sensitive: false },
    ToolDef { name: "sandbox_download_url", brief: "下载网络URL到沙箱", usage: "参数: url(完整URL), name(可选文件名;docker 后端禁用网络,该工具不可用)", sensitive: false },
    ToolDef { name: "sandbox_status", brief: "查询沙箱状态与根目录", usage: "无参数", sensitive: false },
];

fn catalog(mode: AgentMode) -> &'static [ToolDef] {
    match mode {
        AgentMode::QqOps => QQ_TOOLS,
        AgentMode::Sandbox => SANDBOX_TOOLS,
    }
}

fn tool_by_name(mode: AgentMode, name: &str) -> Option<&'static ToolDef> {
    catalog(mode).iter().find(|t| t.name == name)
}

// ---------- 模式 / 状态 / 任务 ----------

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentMode {
    QqOps,
    Sandbox,
}

impl AgentMode {
    pub fn label(&self) -> &'static str {
        match self {
            AgentMode::QqOps => "QQ 操作",
            AgentMode::Sandbox => "沙箱",
        }
    }
}

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// 沙箱模式:搭建中(建目录/探测)
    SandboxSetup,
    /// 沙箱模式:激活中(拉镜像等)
    SandboxActivate,
    /// 阶段一:工具选择
    Selecting,
    /// 阶段二:等待用户审批
    AwaitingApproval,
    /// 阶段二:执行中
    Executing,
    /// 已暂停(审批超时或用户手动;主对话可继续)
    Paused,
    /// 完成
    Done,
    /// 失败
    Failed,
    /// 已停止(用户手动终止)
    Stopped,
}

impl TaskStatus {
    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            TaskStatus::SandboxSetup => "沙箱搭建中",
            TaskStatus::SandboxActivate => "沙箱激活中",
            TaskStatus::Selecting => "工具选择中",
            TaskStatus::AwaitingApproval => "等待审批",
            TaskStatus::Executing => "执行中",
            TaskStatus::Paused => "已暂停",
            TaskStatus::Done => "已完成",
            TaskStatus::Failed => "失败",
            TaskStatus::Stopped => "已停止",
        }
    }
    pub fn terminal(&self) -> bool {
        matches!(self, TaskStatus::Done | TaskStatus::Failed | TaskStatus::Stopped)
    }
}

/// 触发来源与权限信息
#[derive(Serialize, Clone, Debug)]
pub struct OriginInfo {
    pub kind: MsgKind,
    pub group_id: Option<i64>,
    pub user_id: i64,
    /// 来源会话键(与主会话一致:`g{群号}` / `u{QQ号}`;结果注入与汇报定位用)
    pub session_key: String,
    /// 触发者角色: owner / admin / member / trusted(白名单)/ private
    pub requester_role: String,
}

impl OriginInfo {
    fn with_role(mut self, role: String) -> Self {
        self.requester_role = role;
        self
    }
}

/// 待审批的一步
#[derive(Serialize, Clone, Debug)]
pub struct PendingStep {
    pub step_id: String,
    pub tool: String,
    pub params: Value,
    pub note: String,
    pub sensitive: bool,
    /// 权限检查结果描述(展示用)
    pub permission: String,
}

/// 已执行步骤记录
#[derive(Serialize, Clone, Debug)]
pub struct StepRecord {
    pub tool: String,
    pub params: Value,
    pub result: String,
    pub ok: bool,
    pub ts: i64,
}

/// 一个任务(序列化视图即 GUI 渲染数据;内部字段跳过)。
/// Clone/Debug 手写:approval_tx 克隆时丢弃(避免重复唤醒),backend 无 Debug。
#[derive(Serialize)]
pub struct AgentTask {
    pub id: String,
    pub mode: AgentMode,
    pub goal: String,
    pub origin: OriginInfo,
    pub status: TaskStatus,
    pub steps: Vec<StepRecord>,
    pub pending: Option<PendingStep>,
    pub summary: Option<String>,
    pub error: Option<String>,
    pub rounds: u32,
    pub max_rounds: u32,
    pub selected_tools: Vec<String>,
    pub sandbox_backend: String,
    pub created_ts: i64,
    pub finished_ts: Option<i64>,
    #[serde(skip)]
    pub(crate) msgs: Vec<ApiMessage>,
    #[serde(skip)]
    pub(crate) approval_tx: Option<oneshot::Sender<ApprovalDecision>>,
    #[serde(skip)]
    pub(crate) backend: Option<Arc<Box<dyn SandboxBackend>>>,
}

impl Clone for AgentTask {
    fn clone(&self) -> Self {
        AgentTask {
            id: self.id.clone(),
            mode: self.mode,
            goal: self.goal.clone(),
            origin: self.origin.clone(),
            status: self.status,
            steps: self.steps.clone(),
            pending: self.pending.clone(),
            summary: self.summary.clone(),
            error: self.error.clone(),
            rounds: self.rounds,
            max_rounds: self.max_rounds,
            selected_tools: self.selected_tools.clone(),
            sandbox_backend: self.sandbox_backend.clone(),
            created_ts: self.created_ts,
            finished_ts: self.finished_ts,
            msgs: self.msgs.clone(),
            // 克隆丢弃审批通道:副本不应能唤醒原任务的审批
            approval_tx: None,
            backend: self.backend.clone(),
        }
    }
}

impl std::fmt::Debug for AgentTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentTask")
            .field("id", &self.id)
            .field("mode", &self.mode)
            .field("status", &self.status)
            .field("rounds", &self.rounds)
            .field("goal", &self.goal)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApprovalDecision {
    Approved,
    Rejected,
}

/// runner 相位返回值(循环控制)
enum PhaseNext {
    Continue,
    Paused,
    Finish,
}

// ---------- 管理器 ----------

pub struct AgentManager {
    pub cfg: Arc<RwLock<Config>>,
    pub events: Arc<StdMutex<EventBuf>>,
    pub sender: ActionSender,
    pub llm: LlmClient,
    pub cost: Arc<StdMutex<CostTracker>>,
    /// 沙箱根目录基址(任务目录 = {base}/{task_id})
    pub sandbox_base: PathBuf,
    pub tasks: Mutex<Vec<AgentTask>>,
    /// 每任务的运行标志(防重复 runner)
    running: StdMutex<std::collections::HashMap<String, Arc<AtomicBool>>>,
    /// 每任务的暂停信号
    pauses: StdMutex<std::collections::HashMap<String, watch::Sender<bool>>>,
    /// 每任务的停止令牌
    stops: StdMutex<std::collections::HashMap<String, CancellationToken>>,
    /// 任务结束回调(chat.rs 注册:注入结果摘要 + 触发主模型汇报)
    pub on_finished: StdMutex<Option<Box<dyn Fn(AgentTask) + Send + Sync>>>,
}

impl AgentManager {
    pub fn new(
        cfg: Arc<RwLock<Config>>,
        events: Arc<StdMutex<EventBuf>>,
        sender: ActionSender,
        cost: Arc<StdMutex<CostTracker>>,
        sandbox_base: PathBuf,
    ) -> Self {
        Self {
            cfg,
            events,
            sender,
            llm: LlmClient::new(),
            cost,
            sandbox_base,
            tasks: Mutex::new(Vec::new()),
            running: StdMutex::new(std::collections::HashMap::new()),
            pauses: StdMutex::new(std::collections::HashMap::new()),
            stops: StdMutex::new(std::collections::HashMap::new()),
            on_finished: StdMutex::new(None),
        }
    }

    /// 注册任务结束回调(任一任务进入终态时调用一次)
    pub fn set_on_finished(&self, cb: Box<dyn Fn(AgentTask) + Send + Sync>) {
        *self.on_finished.lock().unwrap() = Some(cb);
    }

    // ---------- 对外命令 ----------

    /// GUI 手动创建任务(用户在客户端面板操作,视为可信;仍校验模式开关)
    pub async fn spawn_trusted(self: &Arc<Self>, mode: AgentMode, goal: &str, session_key: &str) -> Result<String, String> {
        let origin = OriginInfo {
            kind: MsgKind::Private,
            group_id: None,
            user_id: 0,
            session_key: session_key.to_string(),
            requester_role: "trusted".into(),
        };
        self.spawn_inner(mode, goal, origin, true).await
    }

    /// 创建任务(校验模式开关 + 触发者权限),返回任务 ID。
    pub async fn spawn(self: &Arc<Self>, mode: AgentMode, goal: &str, origin: OriginInfo) -> Result<String, String> {
        let (whitelist, roles, allow_private) = {
            let cfg = self.cfg.read().await;
            (cfg.agent.owner_whitelist.clone(), cfg.agent.allowed_group_roles.clone(), cfg.agent.allow_private)
        };
        let role = self.check_requester(&whitelist, &roles, allow_private, &origin).await?;
        self.spawn_inner(mode, goal, origin.with_role(role), false).await
    }

    async fn spawn_inner(self: &Arc<Self>, mode: AgentMode, goal: &str, origin: OriginInfo, trusted: bool) -> Result<String, String> {
        let (enable_qq_ops, enable_sandbox, backend_kind, sb_cfg) = {
            let cfg = self.cfg.read().await;
            let a = cfg.agent.clone();
            (a.enable_qq_ops, a.enable_sandbox, a.sandbox_backend, cfg.sandbox.clone())
        };
        // 模式开关
        match mode {
            AgentMode::QqOps if !enable_qq_ops => return Err("QQ 操作模式已关闭(可在 SubAgent 设置中开启)".into()),
            AgentMode::Sandbox if !enable_sandbox => return Err("沙箱模式已关闭(可在 SubAgent 设置中开启)".into()),
            _ => {}
        }
        let _ = trusted;
        let goal = goal.trim().to_string();
        if goal.is_empty() {
            return Err("任务目标为空".into());
        }

        let id = trace::new_id();
        let now = trace::now_ts();
        let (pause_tx, pause_rx) = watch::channel(false);
        let stop = CancellationToken::new();
        self.pauses.lock().unwrap().insert(id.clone(), pause_tx);
        self.stops.lock().unwrap().insert(id.clone(), stop.clone());

        // 沙箱模式:创建后端并立即「搭建」
        let (backend, backend_name) = if mode == AgentMode::Sandbox {
            let root = self.sandbox_base.join(&id);
            let backend = sandbox::factory(&backend_kind, &sb_cfg, root)?;
            let name = backend.name().to_string();
            (Some(Arc::new(backend)), name)
        } else {
            (None, String::new())
        };

        let mut task = AgentTask {
            id: id.clone(),
            mode,
            goal,
            origin,
            status: if mode == AgentMode::Sandbox {
                TaskStatus::SandboxSetup
            } else {
                TaskStatus::Selecting
            },
            steps: Vec::new(),
            pending: None,
            summary: None,
            error: None,
            rounds: 0,
            max_rounds: 1,
            selected_tools: Vec::new(),
            sandbox_backend: backend_name,
            created_ts: now,
            finished_ts: None,
            msgs: Vec::new(),
            approval_tx: None,
            backend,
        };
        let max_rounds = {
            let cfg = self.cfg.read().await;
            cfg.agent.max_rounds.max(1)
        };
        task.max_rounds = max_rounds;
        task.msgs.push(ApiMessage {
            role: "user".into(),
            content: build_task_brief(&task),
        });
        {
            let mut tasks = self.tasks.lock().await;
            tasks.push(task);
        }
        self.emit_updated(&id).await;
        self.emit_log(
            &id,
            &format!(
                "任务已创建({}),{}",
                mode.label(),
                if mode == AgentMode::Sandbox { "先搭建沙箱" } else { "开始工具选择" }
            ),
        );
        self.run_runner(id.clone(), pause_rx, stop);
        Ok(id)
    }

    /// 等待任务结束(轮询,最多等 max_wait;返回任务克隆)
    pub async fn wait_finished(&self, task_id: &str, max_wait: Duration) -> Option<AgentTask> {
        let deadline = tokio::time::Instant::now() + max_wait;
        loop {
            {
                let tasks = self.tasks.lock().await;
                if let Some(t) = tasks.iter().find(|t| t.id == task_id) {
                    if t.status.terminal() {
                        return Some(t.clone());
                    }
                } else {
                    return None;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// GUI 审批:批准当前待审批步骤
    pub async fn approve_step(&self, task_id: &str, step_id: &str) -> Result<(), String> {
        self.decide_step(task_id, step_id, ApprovalDecision::Approved).await
    }

    /// GUI 审批:拒绝当前待审批步骤
    pub async fn reject_step(&self, task_id: &str, step_id: &str) -> Result<(), String> {
        self.decide_step(task_id, step_id, ApprovalDecision::Rejected).await
    }

    async fn decide_step(&self, task_id: &str, step_id: &str, d: ApprovalDecision) -> Result<(), String> {
        let mut tasks = self.tasks.lock().await;
        let task = tasks
            .iter_mut()
            .find(|t| t.id == task_id)
            .ok_or_else(|| "任务不存在".to_string())?;
        let pending = task
            .pending
            .as_ref()
            .ok_or_else(|| "当前没有待审批的步骤".to_string())?;
        if pending.step_id != step_id {
            return Err("步骤 ID 不匹配".into());
        }
        if let Some(tx) = task.approval_tx.take() {
            let _ = tx.send(d);
        }
        Ok(())
    }

    /// 暂停任务(冻结;主对话继续)
    pub async fn pause_task(&self, task_id: &str) -> Result<(), String> {
        {
            let mut tasks = self.tasks.lock().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.id == task_id)
                .ok_or_else(|| "任务不存在".to_string())?;
            if task.status.terminal() {
                return Err("任务已结束,无法暂停".into());
            }
            if task.status == TaskStatus::Paused {
                return Err("任务已处于暂停状态".into());
            }
            task.status = TaskStatus::Paused;
        }
        if let Some(tx) = self.pauses.lock().unwrap().get(task_id) {
            let _ = tx.send(true);
        }
        self.emit_log(task_id, "⏸ 已暂停(主对话可继续)");
        self.emit_updated(task_id).await;
        Ok(())
    }

    /// 恢复暂停的任务
    pub async fn resume_task(self: &Arc<Self>, task_id: &str) -> Result<(), String> {
        {
            let mut tasks = self.tasks.lock().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.id == task_id)
                .ok_or_else(|| "任务不存在".to_string())?;
            if task.status != TaskStatus::Paused {
                return Err("任务不在暂停状态".into());
            }
            task.status = if task.pending.is_some() {
                TaskStatus::AwaitingApproval
            } else {
                TaskStatus::Executing
            };
        }
        if let Some(tx) = self.pauses.lock().unwrap().get(task_id) {
            let _ = tx.send(false);
        }
        self.emit_log(task_id, "▶ 已恢复");
        self.emit_updated(task_id).await;
        let (pause_rx, stop) = {
            let pauses = self.pauses.lock().unwrap();
            let stops = self.stops.lock().unwrap();
            let pr = pauses.get(task_id).map(|s| s.subscribe()).unwrap_or_else(|| watch::channel(false).1);
            let st = stops.get(task_id).cloned().unwrap_or_else(CancellationToken::new);
            (pr, st)
        };
        self.run_runner(task_id.to_string(), pause_rx, stop);
        Ok(())
    }

    /// 停止任务(终止 + 清理沙箱)
    pub async fn stop_task(&self, task_id: &str) -> Result<(), String> {
        let destroy = {
            let mut tasks = self.tasks.lock().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.id == task_id)
                .ok_or_else(|| "任务不存在".to_string())?;
            if task.status.terminal() {
                return Err("任务已结束".into());
            }
            task.status = TaskStatus::Stopped;
            task.finished_ts = Some(trace::now_ts());
            task.approval_tx = None;
            task.pending = None;
            self.cfg.read().await.sandbox.destroy_on_done && task.backend.is_some()
        };
        if let Some(stop) = self.stops.lock().unwrap().get(task_id) {
            stop.cancel();
        }
        if destroy {
            self.destroy_backend(task_id).await;
        }
        self.emit_log(task_id, "⛔ 已停止");
        self.emit_updated(task_id).await;
        Ok(())
    }

    /// 任务列表视图(按创建时间倒序)
    pub async fn list(&self) -> Vec<AgentTask> {
        let tasks = self.tasks.lock().await;
        let mut v: Vec<AgentTask> = tasks.iter().cloned().collect();
        v.sort_by_key(|t| std::cmp::Reverse(t.created_ts));
        v
    }

    /// 移除已结束任务(清列表)
    pub async fn remove_task(&self, task_id: &str) -> Result<(), String> {
        let mut tasks = self.tasks.lock().await;
        let idx = tasks
            .iter()
            .position(|t| t.id == task_id && t.status.terminal())
            .ok_or_else(|| "任务不存在或未结束".to_string())?;
        tasks.remove(idx);
        self.running.lock().unwrap().remove(task_id);
        self.pauses.lock().unwrap().remove(task_id);
        self.stops.lock().unwrap().remove(task_id);
        Ok(())
    }

    // ---------- 权限 ----------

    /// 触发者权限校验:群内查角色,私聊走白名单/开关。返回角色字符串。
    async fn check_requester(
        &self,
        whitelist: &str,
        allowed_roles: &str,
        allow_private: bool,
        origin: &OriginInfo,
    ) -> Result<String, String> {
        let whitelisted = whitelist
            .split(',')
            .map(|s| s.trim())
            .any(|s| !s.is_empty() && s == origin.user_id.to_string());
        if whitelisted {
            return Ok("trusted".into());
        }
        match origin.kind {
            MsgKind::Private => {
                if allow_private {
                    Ok("private".into())
                } else {
                    Err("私聊触发未开启,或你的 QQ 不在白名单内".into())
                }
            }
            MsgKind::Group => {
                let gid = origin.group_id.unwrap_or(0);
                if gid <= 0 {
                    return Err("缺少群号".into());
                }
                let data = self
                    .sender
                    .send("get_group_member_info", json!({ "group_id": gid, "user_id": origin.user_id }))
                    .await
                    .unwrap_or_default();
                let role = data["role"].as_str().unwrap_or("member").to_string();
                let allowed: Vec<&str> = allowed_roles
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();
                if allowed.is_empty() {
                    Err("仅白名单 QQ 可以发起任务".into())
                } else if allowed.iter().any(|r| *r == role) {
                    Ok(role)
                } else {
                    Err(format!("仅{}(角色)可以发起任务", allowed.join("/")))
                }
            }
        }
    }

    /// 敏感工具门槛:群主/管理员(含白名单 trusted)
    fn sensitive_allowed(role: &str) -> bool {
        matches!(role, "owner" | "admin" | "trusted")
    }

    // ---------- 运行器 ----------

    fn run_runner(self: &Arc<Self>, task_id: String, pause_rx: watch::Receiver<bool>, stop: CancellationToken) {
        {
            let mut running = self.running.lock().unwrap();
            if let Some(flag) = running.get(&task_id) {
                if flag.load(Ordering::Relaxed) {
                    return; // 已有 runner
                }
            }
            running.insert(task_id.clone(), Arc::new(AtomicBool::new(true)));
        }
        let me = self.clone();
        tauri::async_runtime::spawn(async move {
            me.runner_loop(&task_id, pause_rx, stop).await;
        });
    }

    async fn runner_loop(&self, task_id: &str, mut pause_rx: watch::Receiver<bool>, stop: CancellationToken) {
        loop {
            if stop.is_cancelled() {
                break;
            }
            if *pause_rx.borrow() {
                let mut tasks = self.tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                    if !t.status.terminal() && t.status != TaskStatus::Paused {
                        t.status = TaskStatus::Paused;
                        t.approval_tx = None;
                    }
                }
                drop(tasks);
                self.emit_updated(task_id).await;
                break;
            }
            // 任务总超时
            {
                let tasks = self.tasks.lock().await;
                if let Some(t) = tasks.iter().find(|t| t.id == task_id) {
                    let timeout = self.cfg.read().await.agent.task_timeout_secs.max(60);
                    if trace::now_ts() - t.created_ts > timeout as i64 {
                        drop(tasks);
                        self.fail_task(task_id, &format!("任务总超时({timeout}s)")).await;
                        break;
                    }
                }
            }
            let status = {
                let tasks = self.tasks.lock().await;
                tasks.iter().find(|t| t.id == task_id).map(|t| t.status)
            };
            let next = match status {
                Some(TaskStatus::SandboxSetup) => self.phase_sandbox_setup(task_id).await,
                Some(TaskStatus::SandboxActivate) => self.phase_sandbox_activate(task_id).await,
                Some(TaskStatus::Selecting) => self.phase_select(task_id).await,
                Some(TaskStatus::Executing) | Some(TaskStatus::AwaitingApproval) => {
                    self.phase_step(task_id, &mut pause_rx, &stop).await
                }
                Some(TaskStatus::Paused) => break,
                Some(s) if s.terminal() => break,
                _ => break,
            };
            match next {
                PhaseNext::Continue => {}
                PhaseNext::Paused => break,
                PhaseNext::Finish => break,
            }
        }
        if let Some(flag) = self.running.lock().unwrap().get(task_id) {
            flag.store(false, Ordering::Relaxed);
        }
        // 终态兜底清理沙箱
        let (terminal, destroy) = {
            let tasks = self.tasks.lock().await;
            match tasks.iter().find(|t| t.id == task_id) {
                Some(t) if t.status.terminal() => {
                    let destroy = self.cfg.read().await.sandbox.destroy_on_done && t.backend.is_some();
                    (true, destroy)
                }
                _ => (false, false),
            }
        };
        if terminal && destroy {
            self.destroy_backend(task_id).await;
        }
        // 任务结束回调(仅一次;由 chat.rs 注入结果 + 触发主模型汇报)
        if terminal {
            let view = {
                let tasks = self.tasks.lock().await;
                tasks.iter().find(|t| t.id == task_id).cloned()
            };
            if let Some(view) = view {
                if let Some(cb) = self.on_finished.lock().unwrap().as_ref() {
                    cb(view);
                }
            }
        }
    }

    // ---- 沙箱搭建 / 激活 ----

    async fn phase_sandbox_setup(&self, task_id: &str) -> PhaseNext {
        self.emit_log(task_id, "🛠 正在搭建沙箱…");
        let (result, name) = {
            let tasks = self.tasks.lock().await;
            let t = tasks.iter().find(|t| t.id == task_id);
            match t.and_then(|t| t.backend.as_ref()) {
                Some(b) => {
                    let r = b.setup().await;
                    (r, b.name().to_string())
                }
                None => (Err("沙箱后端缺失".into()), String::new()),
            }
        };
        match result {
            Ok(()) => {
                self.emit_log(task_id, &format!("✅ 沙箱搭建完成({name}),开始激活…"));
                {
                    let mut tasks = self.tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                        t.status = TaskStatus::SandboxActivate;
                    }
                }
                self.emit_updated(task_id).await;
                PhaseNext::Continue
            }
            Err(e) => {
                self.fail_task(task_id, &format!("沙箱搭建失败: {e}")).await;
                PhaseNext::Finish
            }
        }
    }

    async fn phase_sandbox_activate(&self, task_id: &str) -> PhaseNext {
        self.emit_log(task_id, "🚀 正在激活沙箱…");
        let result = {
            let tasks = self.tasks.lock().await;
            let t = tasks.iter().find(|t| t.id == task_id);
            match t.and_then(|t| t.backend.as_ref()) {
                Some(b) => b.activate().await,
                None => Err("沙箱后端缺失".into()),
            }
        };
        match result {
            Ok(()) => {
                self.emit_log(task_id, "✅ 沙箱已激活,开始工具选择");
                {
                    let mut tasks = self.tasks.lock().await;
                    if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                        t.status = TaskStatus::Selecting;
                    }
                }
                self.emit_updated(task_id).await;
                PhaseNext::Continue
            }
            Err(e) => {
                self.fail_task(task_id, &format!("沙箱激活失败: {e}")).await;
                PhaseNext::Finish
            }
        }
    }

    // ---- 阶段一:工具选择 ----

    async fn phase_select(&self, task_id: &str) -> PhaseNext {
        self.emit_log(task_id, "🧭 工具选择中…");
        let (sys, task_msgs, model, budget) = {
            let tasks = self.tasks.lock().await;
            let t = tasks.iter().find(|t| t.id == task_id);
            let mode = t.map(|t| t.mode).unwrap_or(AgentMode::QqOps);
            let role = t.map(|t| t.origin.requester_role.clone()).unwrap_or_default();
            let msgs = t.map(|t| t.msgs.clone()).unwrap_or_default();
            let model = self.cfg.read().await.active_model().cloned();
            let budget = self.cfg.read().await.agent.select_max_tokens.max(64);
            (select_sys(mode, &role), msgs, model, budget)
        };
        let Some(model) = model else {
            self.fail_task(task_id, "没有可用模型").await;
            return PhaseNext::Finish;
        };
        let mut msgs = vec![ApiMessage { role: "system".into(), content: sys }];
        msgs.extend(task_msgs);
        let mut model2 = model.clone();
        model2.thinking = "disabled".into();
        let reply = match self.llm.chat(&model2, &msgs, Some(budget)).await {
            Ok(r) => {
                self.record_usage(&model, &r.usage);
                r
            }
            Err(e) => {
                self.emit_log(task_id, &format!("选择调用失败,重试: {e}"));
                match self.llm.chat(&model2, &msgs, Some(budget)).await {
                    Ok(r) => {
                        self.record_usage(&model, &r.usage);
                        r
                    }
                    Err(e2) => {
                        self.fail_task(task_id, &format!("工具选择调用失败: {e2}")).await;
                        return PhaseNext::Finish;
                    }
                }
            }
        };
        match parse_select(&reply.text) {
            Ok((tools, plan)) => {
                if tools.is_empty() {
                    let summary = if plan.is_empty() {
                        "任务无法完成:未选择任何工具".to_string()
                    } else {
                        format!("任务无法完成: {plan}")
                    };
                    self.finish_task(task_id, &summary).await;
                    return PhaseNext::Finish;
                }
                let (valid, invalid) = {
                    let tasks = self.tasks.lock().await;
                    let t = tasks.iter().find(|t| t.id == task_id).unwrap();
                    let mode = t.mode;
                    tools
                        .into_iter()
                        .partition::<Vec<String>, _>(|name| tool_by_name(mode, name).is_some())
                };
                if valid.is_empty() {
                    self.fail_task(task_id, &format!("模型选择了无效工具: {invalid:?}")).await;
                    return PhaseNext::Finish;
                }
                let selected = valid.clone();
                {
                    let mut tasks = self.tasks.lock().await;
                    let t = tasks.iter_mut().find(|t| t.id == task_id).unwrap();
                    t.selected_tools = valid;
                    t.status = TaskStatus::Executing;
                    let exec_sys = exec_sys(t.mode, &t.selected_tools);
                    let mut msgs = vec![ApiMessage { role: "system".into(), content: exec_sys }];
                    msgs.extend(t.msgs.drain(..));
                    t.msgs = msgs;
                }
                self.emit_log(
                    task_id,
                    &format!(
                        "🔧 已选择工具: {}{}",
                        selected.join(", "),
                        if invalid.is_empty() { String::new() } else { format!("(忽略无效: {invalid:?})") }
                    ),
                );
                self.emit_updated(task_id).await;
                PhaseNext::Continue
            }
            Err(e) => {
                self.emit_log(task_id, &format!("工具选择输出解析失败: {e}"));
                let mut tasks = self.tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                    t.msgs.push(ApiMessage {
                        role: "user".into(),
                        content: "上一次输出无法解析为 JSON,请严格只输出一个 JSON 对象: {\"tools\": [...], \"plan\": \"...\"}".into(),
                    });
                }
                drop(tasks);
                PhaseNext::Continue
            }
        }
    }

    // ---- 阶段二:执行 ----

    async fn phase_step(&self, task_id: &str, pause_rx: &mut watch::Receiver<bool>, stop: &CancellationToken) -> PhaseNext {
        // 待审批步骤存在时,先等审批
        let has_pending = {
            let tasks = self.tasks.lock().await;
            tasks
                .iter()
                .find(|t| t.id == task_id)
                .map(|t| t.pending.is_some())
                .unwrap_or(false)
        };
        if has_pending {
            return self.await_approval(task_id, pause_rx, stop).await;
        }

        // 轮次上限
        let (over, rounds, max_rounds) = {
            let tasks = self.tasks.lock().await;
            match tasks.iter().find(|t| t.id == task_id) {
                Some(t) => (t.rounds >= t.max_rounds, t.rounds, t.max_rounds),
                None => (true, 0, 0),
            }
        };
        if over {
            self.finish_task(task_id, &format!("已达到执行轮次上限({max_rounds}),任务结束")).await;
            return PhaseNext::Finish;
        }

        // 调用执行 LLM
        self.emit_log(task_id, &format!("⏳ 执行中(第 {}/{} 轮)…", rounds + 1, max_rounds));
        let (msgs, model, budget) = {
            let tasks = self.tasks.lock().await;
            let t = tasks.iter().find(|t| t.id == task_id);
            let msgs = t.map(|t| t.msgs.clone()).unwrap_or_default();
            let model = self.cfg.read().await.active_model().cloned();
            let budget = self.cfg.read().await.agent.step_max_tokens.max(64);
            (msgs, model, budget)
        };
        let Some(model) = model else {
            self.fail_task(task_id, "没有可用模型").await;
            return PhaseNext::Finish;
        };
        let mut model2 = model.clone();
        model2.thinking = "disabled".into();
        let reply = match self.llm.chat(&model2, &msgs, Some(budget)).await {
            Ok(r) => {
                self.record_usage(&model, &r.usage);
                r
            }
            Err(e) => {
                self.emit_log(task_id, &format!("执行调用失败,重试: {e}"));
                match self.llm.chat(&model2, &msgs, Some(budget)).await {
                    Ok(r) => {
                        self.record_usage(&model, &r.usage);
                        r
                    }
                    Err(e2) => {
                        self.fail_task(task_id, &format!("执行调用失败: {e2}")).await;
                        return PhaseNext::Finish;
                    }
                }
            }
        };

        match parse_step(&reply.text) {
            StepOutput::Done { summary } => {
                self.finish_task(task_id, &summary).await;
                PhaseNext::Finish
            }
            StepOutput::Call { tool, params, note } => {
                let (tool_exists, sensitive_ok, is_sensitive, selected, mode) = {
                    let tasks = self.tasks.lock().await;
                    let t = tasks.iter().find(|t| t.id == task_id).unwrap();
                    let def = tool_by_name(t.mode, &tool);
                    let exists = def.is_some();
                    let sens = def.map(|d| d.sensitive).unwrap_or(false);
                    let ok = !sens || Self::sensitive_allowed(&t.origin.requester_role);
                    (exists, ok, sens, t.selected_tools.contains(&tool), t.mode)
                };
                if !tool_exists {
                    self.push_feedback(task_id, &format!("工具「{tool}」不存在,请从目录中选择")).await;
                    return PhaseNext::Continue;
                }
                if !sensitive_ok {
                    self.push_feedback(task_id, &format!("工具「{tool}」为敏感操作,仅群主/管理员(或白名单)可用,请换方案或直接 done")).await;
                    return PhaseNext::Continue;
                }
                if !selected {
                    let usage = tool_by_name(mode, &tool).map(|d| d.usage).unwrap_or("");
                    self.append_usage(task_id, &tool, usage).await;
                }
                let step_id = trace::new_id();
                let permission = if is_sensitive {
                    "敏感操作 · 仅群主/管理员 · 请谨慎审批".to_string()
                } else {
                    "常规操作".to_string()
                };
                {
                    let mut tasks = self.tasks.lock().await;
                    let t = tasks.iter_mut().find(|t| t.id == task_id).unwrap();
                    t.pending = Some(PendingStep {
                        step_id: step_id.clone(),
                        tool: tool.clone(),
                        params: params.clone(),
                        note,
                        sensitive: is_sensitive,
                        permission: permission.clone(),
                    });
                    t.status = TaskStatus::AwaitingApproval;
                }
                self.emit_log(
                    task_id,
                    &format!("📋 等待审批: {tool}{}", if is_sensitive { " (敏感)" } else { "" }),
                );
                self.emit_updated(task_id).await;
                PhaseNext::Continue
            }
            StepOutput::Invalid(e) => {
                self.push_feedback(task_id, &format!("输出无法解析({e}),请严格只输出一个 JSON 对象")).await;
                PhaseNext::Continue
            }
        }
    }

    /// 等待用户审批(批准→执行;拒绝→反馈;超时→自动暂停)
    async fn await_approval(&self, task_id: &str, pause_rx: &mut watch::Receiver<bool>, stop: &CancellationToken) -> PhaseNext {
        let rx = {
            let mut tasks = self.tasks.lock().await;
            let t = tasks.iter_mut().find(|t| t.id == task_id).unwrap();
            let (tx, rx) = oneshot::channel();
            t.approval_tx = Some(tx);
            rx
        };
        let timeout = self.cfg.read().await.agent.approval_timeout_secs.max(30);
        self.emit_updated(task_id).await;

        let decision = tokio::select! {
            _ = stop.cancelled() => {
                let mut tasks = self.tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                    t.approval_tx = None;
                }
                return PhaseNext::Finish;
            }
            _ = pause_rx.changed() => {
                let mut tasks = self.tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                    t.approval_tx = None;
                    t.status = TaskStatus::Paused;
                }
                self.emit_updated(task_id).await;
                return PhaseNext::Paused;
            }
            d = rx => match d {
                Ok(ApprovalDecision::Approved) => ApprovalDecision::Approved,
                Ok(ApprovalDecision::Rejected) => ApprovalDecision::Rejected,
                Err(_) => ApprovalDecision::Rejected,
            },
            _ = tokio::time::sleep(Duration::from_secs(timeout)) => {
                let mut tasks = self.tasks.lock().await;
                if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                    t.approval_tx = None;
                    t.status = TaskStatus::Paused;
                }
                self.emit_log(task_id, &format!("⏸ 审批超时({timeout}s),任务自动暂停"));
                self.emit_updated(task_id).await;
                return PhaseNext::Paused;
            }
        };

        let (step, mode, backend) = {
            let mut tasks = self.tasks.lock().await;
            let t = tasks.iter_mut().find(|t| t.id == task_id).unwrap();
            t.approval_tx = None;
            let step = t.pending.take();
            (step, t.mode, t.backend.clone())
        };
        let Some(step) = step else {
            return PhaseNext::Continue;
        };

        if decision == ApprovalDecision::Rejected {
            self.push_feedback(
                task_id,
                &format!("用户拒绝了上一步操作(工具「{}」)。请调整方案或直接输出 done 结束任务。", step.tool),
            ).await;
            self.emit_log(task_id, &format!("❌ 已拒绝: {}", step.tool));
            return PhaseNext::Continue;
        }

        // 批准:执行
        self.emit_log(task_id, &format!("✅ 已批准: {}", step.tool));
        let result = match mode {
            AgentMode::QqOps => self.execute_qq_tool(task_id, &step.tool, &step.params).await,
            AgentMode::Sandbox => {
                self.execute_sandbox_tool(task_id, &step.tool, &step.params, backend.as_deref()).await
            }
        };
        let (result_text, ok) = match &result {
            Ok(v) => (v.clone(), true),
            Err(e) => (e.clone(), false),
        };
        {
            let mut tasks = self.tasks.lock().await;
            let t = tasks.iter_mut().find(|t| t.id == task_id).unwrap();
            t.steps.push(StepRecord {
                tool: step.tool.clone(),
                params: step.params.clone(),
                result: result_text.clone(),
                ok,
                ts: trace::now_ts(),
            });
            t.rounds += 1;
            t.status = TaskStatus::Executing;
            t.msgs.push(ApiMessage {
                role: "assistant".into(),
                content: format!(
                    "工具调用: {} 参数: {}{}",
                    step.tool,
                    serde_json::to_string(&step.params).unwrap_or_default(),
                    if step.note.is_empty() { String::new() } else { format!("(说明:{})", step.note) }
                ),
            });
            t.msgs.push(ApiMessage {
                role: "user".into(),
                content: format!("工具「{}」执行结果: {}", step.tool, truncate(&result_text, 2000)),
            });
        }
        if !ok {
            self.emit_log(task_id, &format!("⚠️ 工具执行失败: {result_text}"));
        }
        self.emit_updated(task_id).await;
        PhaseNext::Continue
    }

    // ---------- 工具执行器 ----------

    async fn execute_qq_tool(&self, task_id: &str, tool: &str, params: &Value) -> Result<String, String> {
        let origin = {
            let tasks = self.tasks.lock().await;
            tasks.iter().find(|t| t.id == task_id).map(|t| t.origin.clone()).unwrap()
        };
        let sender = &self.sender;
        match tool {
            "send_group_msg" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                if origin.kind == MsgKind::Group && Some(gid) != origin.group_id {
                    return Err(format!("只能向触发来源群({})发送消息", origin.group_id.unwrap_or(0)));
                }
                let message = params["message"].clone();
                if message.is_null() {
                    return Err("缺少 message".into());
                }
                let r = sender.send("send_group_msg", json!({ "group_id": gid, "message": message })).await?;
                Ok(format!("已发送,message_id={}", r["message_id"]))
            }
            "send_private_msg" => {
                let uid = params["user_id"].as_i64().ok_or("缺少 user_id")?;
                if origin.kind == MsgKind::Private && uid != origin.user_id {
                    return Err(format!("只能向触发者({})发送私聊消息", origin.user_id));
                }
                let message = params["message"].clone();
                if message.is_null() {
                    return Err("缺少 message".into());
                }
                let r = sender.send("send_private_msg", json!({ "user_id": uid, "message": message })).await?;
                Ok(format!("已发送,message_id={}", r["message_id"]))
            }
            "set_essence_msg" => {
                let mid = params["message_id"].as_i64().ok_or("缺少 message_id")?;
                sender.send("set_essence_msg", json!({ "message_id": mid })).await?;
                Ok("已设置群精华".into())
            }
            "set_friend_remark" => {
                let uid = params["user_id"].as_i64().ok_or("缺少 user_id")?;
                let remark = params["remark"].as_str().ok_or("缺少 remark")?.to_string();
                sender.send("set_friend_remark", json!({ "user_id": uid, "remark": remark })).await?;
                Ok(format!("已将好友 {uid} 备注改为「{remark}」"))
            }
            "set_group_card" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                let uid = params["user_id"].as_i64().ok_or("缺少 user_id")?;
                let card = params["card"].as_str().unwrap_or("").to_string();
                sender.send("set_group_card", json!({ "group_id": gid, "user_id": uid, "card": card })).await?;
                Ok(format!("已将 {uid} 的群名片改为「{card}」"))
            }
            "send_group_sign" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                sender.send("send_group_sign", json!({ "group_id": gid })).await?;
                Ok(format!("群 {gid} 打卡成功"))
            }
            "get_msg" => {
                let mid = params["message_id"].as_i64().ok_or("缺少 message_id")?;
                let r = sender.send("get_msg", json!({ "message_id": mid })).await?;
                Ok(serde_json::to_string(&r).unwrap_or_default())
            }
            "get_group_msg_history" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                let count = params["count"].as_i64().unwrap_or(10).clamp(1, 50);
                let r = sender.send("get_group_msg_history", json!({ "group_id": gid, "count": count })).await?;
                let msgs = r["messages"].as_array().cloned().unwrap_or_default();
                if msgs.is_empty() {
                    return Ok("(无历史消息)".into());
                }
                let mut out = String::new();
                for m in msgs.iter().rev().take(count as usize) {
                    let uid = m["user_id"].as_i64().unwrap_or(0);
                    let text = crate::napcat::extract_text_for_display(m);
                    out.push_str(&format!("[QQ{uid}] {text}\n"));
                }
                Ok(out.trim().to_string())
            }
            "get_group_member_info" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                let uid = params["user_id"].as_i64().ok_or("缺少 user_id")?;
                let r = sender.send("get_group_member_info", json!({ "group_id": gid, "user_id": uid })).await?;
                Ok(serde_json::to_string(&r).unwrap_or_default())
            }
            "send_poke" => {
                let uid = params["user_id"].as_i64().ok_or("缺少 user_id")?;
                let mut p = json!({ "user_id": uid });
                if let Some(gid) = params["group_id"].as_i64() {
                    p["group_id"] = json!(gid);
                }
                sender.send("send_poke", p).await?;
                Ok(format!("已戳 {uid}"))
            }
            "upload_group_file" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                let file = params["file"].as_str().ok_or("缺少 file")?.to_string();
                let name = params["name"].as_str().unwrap_or("file").to_string();
                sender.send("upload_group_file", json!({ "group_id": gid, "file": file, "name": name })).await?;
                Ok(format!("已上传群文件「{name}」"))
            }
            "get_file" => {
                let fid = params["file_id"].as_str().ok_or("缺少 file_id")?;
                let r = sender.send("get_file", json!({ "file_id": fid })).await?;
                Ok(serde_json::to_string(&r).unwrap_or_default())
            }
            "delete_msg" => {
                let mid = params["message_id"].as_i64().ok_or("缺少 message_id")?;
                sender.send("delete_msg", json!({ "message_id": mid })).await?;
                Ok(format!("已撤回消息 {mid}"))
            }
            "set_group_ban" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                let uid = params["user_id"].as_i64().ok_or("缺少 user_id")?;
                let dur = params["duration"].as_i64().unwrap_or(0);
                sender.send("set_group_ban", json!({ "group_id": gid, "user_id": uid, "duration": dur })).await?;
                Ok(if dur == 0 { format!("已解除 {uid} 禁言") } else { format!("已禁言 {uid} {dur} 秒") })
            }
            "set_group_whole_ban" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                let enable = params["enable"].as_bool().ok_or("缺少 enable")?;
                sender.send("set_group_whole_ban", json!({ "group_id": gid, "enable": enable })).await?;
                Ok(if enable { "已全体禁言".into() } else { "已解除全体禁言".into() })
            }
            "set_group_kick" => {
                let gid = params["group_id"].as_i64().ok_or("缺少 group_id")?;
                let uid = params["user_id"].as_i64().ok_or("缺少 user_id")?;
                let mut p = json!({ "group_id": gid, "user_id": uid });
                if let Some(r) = params["reject_add_request"].as_bool() {
                    p["reject_add_request"] = json!(r);
                }
                sender.send("set_group_kick", p).await?;
                Ok(format!("已将 {uid} 移出群 {gid}"))
            }
            _ => Err(format!("未知 QQ 工具: {tool}")),
        }
    }

    async fn execute_sandbox_tool(
        &self,
        _task_id: &str,
        tool: &str,
        params: &Value,
        backend: Option<&Box<dyn SandboxBackend>>,
    ) -> Result<String, String> {
        let backend = backend.ok_or("沙箱未就绪")?;
        let root = backend.root().to_path_buf();
        match tool {
            "sandbox_status" => Ok(format!(
                "沙箱就绪: 后端={} 根目录={}",
                backend.name(),
                root.display()
            )),
            "sandbox_read_file" => {
                let path = params["path"].as_str().ok_or("缺少 path")?;
                let full = sandbox::resolve_in_root(&root, path)?;
                let content = std::fs::read_to_string(&full).map_err(|e| format!("读取失败: {e}"))?;
                Ok(truncate(&content, 4000))
            }
            "sandbox_write_file" => {
                let path = params["path"].as_str().ok_or("缺少 path")?;
                let content = params["content"].as_str().unwrap_or("");
                let full = sandbox::resolve_in_root(&root, path)?;
                if let Some(parent) = full.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| format!("创建目录失败: {e}"))?;
                }
                std::fs::write(&full, content).map_err(|e| format!("写入失败: {e}"))?;
                Ok(format!("已写入 {}({} 字符)", path, content.chars().count()))
            }
            "sandbox_list_dir" => {
                let full = match params["path"].as_str() {
                    Some(p) if !p.trim().is_empty() => sandbox::resolve_in_root(&root, p)?,
                    _ => root.clone(),
                };
                let mut names = Vec::new();
                for e in std::fs::read_dir(&full).map_err(|e| format!("列目录失败: {e}"))? {
                    let e = e.map_err(|e| format!("列目录失败: {e}"))?;
                    let is_dir = e.file_type().map(|t| t.is_dir()).unwrap_or(false);
                    names.push(format!(
                        "{}{}",
                        e.file_name().to_string_lossy(),
                        if is_dir { "/" } else { "" }
                    ));
                }
                if names.is_empty() {
                    Ok("(空目录)".into())
                } else {
                    Ok(format!("{} 项: {}", names.len(), names.join(" ")))
                }
            }
            "sandbox_run_cmd" => {
                let cmd = params["cmd"].as_str().ok_or("缺少 cmd")?;
                let args: Vec<String> = params["args"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect())
                    .unwrap_or_default();
                let workdir = root.join("work");
                let out = backend.run_cmd(&workdir, cmd, &args).await?;
                Ok(out.summary(2000))
            }
            "sandbox_download_qq_file" => {
                let fid = params["file_id"].as_str().ok_or("缺少 file_id")?;
                let r = self.sender.send("get_file", json!({ "file_id": fid })).await?;
                let path = r["file"].as_str().or(r["path"].as_str()).ok_or("get_file 未返回文件路径")?;
                let src = Path::new(path);
                if !src.exists() {
                    return Err(format!("文件不存在于本机: {path}(NapCat 与 LightBot 需在同一台机器)"));
                }
                let fname = src
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".into());
                let dst = root.join("downloads").join(&fname);
                std::fs::create_dir_all(root.join("downloads")).map_err(|e| e.to_string())?;
                std::fs::copy(src, &dst).map_err(|e| format!("复制失败: {e}"))?;
                Ok(format!("已下载到沙箱: downloads/{fname}"))
            }
            "sandbox_send_file" => {
                let target_type = params["target_type"].as_str().ok_or("缺少 target_type")?;
                let target_id = params["target_id"].as_i64().ok_or("缺少 target_id")?;
                let path = params["path"].as_str().ok_or("缺少 path")?;
                let full = sandbox::resolve_in_root(&root, path)?;
                if !full.exists() {
                    return Err(format!("文件不存在: {path}"));
                }
                let name = params["name"]
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        full.file_name()
                            .map(|s| s.to_string_lossy().to_string())
                            .unwrap_or_else(|| "file".into())
                    });
                match target_type {
                    "group" => {
                        self.sender
                            .send(
                                "upload_group_file",
                                json!({ "group_id": target_id, "file": full.to_string_lossy(), "name": name }),
                            )
                            .await?;
                        Ok(format!("已发送到群 {target_id}: {name}"))
                    }
                    "private" => {
                        self.sender
                            .send(
                                "upload_private_file",
                                json!({ "user_id": target_id, "file": full.to_string_lossy(), "name": name }),
                            )
                            .await?;
                        Ok(format!("已私聊发送给 {target_id}: {name}"))
                    }
                    other => Err(format!("target_type 必须为 group 或 private,得到: {other}")),
                }
            }
            "sandbox_download_url" => {
                let url = params["url"].as_str().ok_or("缺少 url")?;
                let name = params["name"].as_str().map(|s| s.to_string()).unwrap_or_else(|| {
                    url.rsplit('/')
                        .next()
                        .filter(|s| !s.is_empty())
                        .unwrap_or("download")
                        .to_string()
                });
                let resp = self.llm.http_get(url).await.map_err(|e| format!("下载失败: {e}"))?;
                let dst = root.join("downloads").join(&name);
                std::fs::create_dir_all(root.join("downloads")).map_err(|e| e.to_string())?;
                std::fs::write(&dst, resp).map_err(|e| format!("写入失败: {e}"))?;
                Ok(format!("已下载到沙箱: downloads/{name}"))
            }
            _ => Err(format!("未知沙箱工具: {tool}")),
        }
    }

    // ---------- 内部辅助 ----------

    fn record_usage(&self, model: &crate::config::ModelConfig, usage: &crate::llm::Usage) {
        let mut tracker = self.cost.lock().unwrap();
        tracker.record(UsageRecord {
            ts: trace::now_ts(),
            model: model.name.clone(),
            category: "agent".into(),
            prompt: usage.prompt_tokens,
            cache_hit: usage.cache_hit,
            cache_miss: usage.cache_miss,
            completion: usage.completion_tokens,
            reasoning: usage.reasoning_tokens,
            price_input: model.price_input,
            price_cache_hit: model.price_cache_hit,
            price_output: model.price_output,
        });
        self.events.lock().unwrap().push(FrontendEvent::LlmStats {
            model: model.name.clone(),
            prompt_tokens: usage.prompt_tokens,
            completion_tokens: usage.completion_tokens,
            cache_hit: usage.cache_hit,
            cache_miss: usage.cache_miss,
            reasoning_tokens: usage.reasoning_tokens,
            elapsed_ms: 0,
        });
    }

    async fn push_feedback(&self, task_id: &str, text: &str) {
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                t.msgs.push(ApiMessage {
                    role: "user".into(),
                    content: text.to_string(),
                });
                t.status = TaskStatus::Executing;
            }
        }
        self.emit_log(task_id, text);
        self.emit_updated(task_id).await;
    }

    async fn append_usage(&self, task_id: &str, tool: &str, usage: &str) {
        let mut tasks = self.tasks.lock().await;
        if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
            if let Some(first) = t.msgs.first_mut() {
                if first.role == "system" {
                    first
                        .content
                        .push_str(&format!("\n【补充工具:{tool}】{usage}"));
                }
            }
            if !t.selected_tools.contains(&tool.to_string()) {
                t.selected_tools.push(tool.to_string());
            }
        }
    }

    async fn finish_task(&self, task_id: &str, summary: &str) {
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                t.status = TaskStatus::Done;
                t.summary = Some(summary.to_string());
                t.finished_ts = Some(trace::now_ts());
                t.approval_tx = None;
                t.pending = None;
            }
        }
        self.emit_log(task_id, &format!("🏁 任务完成: {summary}"));
        self.emit_updated(task_id).await;
    }

    async fn fail_task(&self, task_id: &str, err: &str) {
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(t) = tasks.iter_mut().find(|t| t.id == task_id) {
                t.status = TaskStatus::Failed;
                t.error = Some(err.to_string());
                t.finished_ts = Some(trace::now_ts());
                t.approval_tx = None;
                t.pending = None;
            }
        }
        self.emit_log(task_id, &format!("❌ 任务失败: {err}"));
        self.emit_updated(task_id).await;
    }

    async fn destroy_backend(&self, task_id: &str) {
        let result = {
            let tasks = self.tasks.lock().await;
            match tasks.iter().find(|t| t.id == task_id).and_then(|t| t.backend.as_ref()) {
                Some(b) => b.destroy(),
                None => Ok(()),
            }
        };
        if let Err(e) = result {
            self.emit_log(task_id, &format!("沙箱清理失败: {e}"));
        }
    }

    async fn emit_updated(&self, task_id: &str) {
        let view = {
            let tasks = self.tasks.lock().await;
            tasks.iter().find(|t| t.id == task_id).cloned()
        };
        if let Some(view) = view {
            self.events.lock().unwrap().push(FrontendEvent::AgentTaskUpdated { task: view });
        }
    }

    fn emit_log(&self, task_id: &str, msg: &str) {
        self.events.lock().unwrap().push(FrontendEvent::AgentLog {
            task_id: task_id.to_string(),
            msg: msg.to_string(),
        });
        self.events.lock().unwrap().push(FrontendEvent::Log {
            level: "info".into(),
            msg: format!("[SubAgent {task_id}] {msg}"),
        });
    }
}

// ---------- 提示词构造 ----------

/// 阶段一系统提示:规则 + 工具目录(仅名称+一行简述)
fn select_sys(mode: AgentMode, requester_role: &str) -> String {
    let mut s = format!("{SELECT_RULES}\n\n可用工具({}模式):\n", mode.label());
    for t in catalog(mode) {
        // 敏感工具对非群主/管理员隐藏
        if t.sensitive && !AgentManager::sensitive_allowed(requester_role) {
            continue;
        }
        s.push_str(&format!("- {}: {}\n", t.name, t.brief));
    }
    s
}

/// 阶段二系统提示:规则 + 所选工具的详细用法
fn exec_sys(mode: AgentMode, selected: &[String]) -> String {
    let mut s = format!("{EXEC_RULES}\n\n工具用法({}模式):\n", mode.label());
    for t in catalog(mode) {
        if selected.iter().any(|n| n == t.name) {
            s.push_str(&format!("【{}】\n{}\n", t.name, t.usage));
        }
    }
    s
}

fn build_task_brief(task: &AgentTask) -> String {
    let mut s = format!("【任务目标】\n{}\n\n", task.goal);
    s.push_str(&format!(
        "【触发场景】来源: {} 触发者 QQ: {}(角色:{})",
        match task.origin.kind {
            MsgKind::Group => format!("群 {}", task.origin.group_id.unwrap_or(0)),
            MsgKind::Private => "私聊".to_string(),
        },
        task.origin.user_id,
        task.origin.requester_role
    ));
    if task.mode == AgentMode::Sandbox {
        s.push_str(&format!("\n【沙箱】后端: {}", task.sandbox_backend));
    }
    s
}

// ---------- 输出解析 ----------

fn extract_json(text: &str) -> Option<Value> {
    let start = text.find('{')?;
    let end = text.rfind('}')?;
    if end <= start {
        return None;
    }
    serde_json::from_str(&text[start..=end]).ok()
}

fn parse_select(text: &str) -> Result<(Vec<String>, String), String> {
    let v = extract_json(text).ok_or_else(|| "未找到 JSON".to_string())?;
    let tools = v["tools"]
        .as_array()
        .ok_or_else(|| "缺少 tools 数组".to_string())?
        .iter()
        .filter_map(|t| t.as_str().map(|s| s.to_string()))
        .collect::<Vec<_>>();
    let plan = v["plan"].as_str().unwrap_or("").to_string();
    Ok((tools, plan))
}

enum StepOutput {
    Done { summary: String },
    Call { tool: String, params: Value, note: String },
    Invalid(String),
}

fn parse_step(text: &str) -> StepOutput {
    let v = match extract_json(text) {
        Some(v) => v,
        None => return StepOutput::Invalid("未找到 JSON".into()),
    };
    if let Some(true) = v["done"].as_bool() {
        return StepOutput::Done {
            summary: v["summary"].as_str().unwrap_or("任务完成").to_string(),
        };
    }
    match v["tool"].as_str() {
        Some(tool) if !tool.is_empty() => StepOutput::Call {
            tool: tool.to_string(),
            params: v.get("params").cloned().unwrap_or_else(|| json!({})),
            note: v["note"].as_str().unwrap_or("").to_string(),
        },
        _ => StepOutput::Invalid("缺少 tool 字段".into()),
    }
}

fn truncate(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        s.to_string()
    } else {
        let t: String = s.chars().take(n).collect();
        format!("{t}…")
    }
}

/// 主模型回复中的任务标记解析(与 memory 的 [记忆:] 同款机制)。
/// 返回(剥离标记后的干净文本, 操作列表);未闭合标记视为普通文本。
pub fn parse_task_ops(text: &str) -> (String, Vec<(AgentMode, String)>) {
    let mut clean = String::new();
    let mut ops = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("[任务:") {
        match rest[start..].find(']') {
            Some(i) => {
                clean.push_str(&rest[..start]);
                let tag = &rest[start..start + i + 1];
                let inner = tag.trim_start_matches("[任务:").trim_end_matches(']').trim();
                if let Some(c) = inner.strip_prefix("QQ") {
                    let c = c.trim();
                    if !c.is_empty() {
                        ops.push((AgentMode::QqOps, c.to_string()));
                    } else {
                        clean.push_str(tag);
                    }
                } else if let Some(c) = inner.strip_prefix("沙箱") {
                    let c = c.trim();
                    if !c.is_empty() {
                        ops.push((AgentMode::Sandbox, c.to_string()));
                    } else {
                        clean.push_str(tag);
                    }
                } else {
                    // 未知模式:原样保留
                    clean.push_str(tag);
                }
                rest = &rest[start + i + 1..];
            }
            None => {
                clean.push_str(rest);
                rest = "";
            }
        }
    }
    clean.push_str(rest);
    (clean, ops)
}

// ---------- 测试 ----------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_task_markers() {
        let (clean, ops) = parse_task_ops("好的,我来处理。[任务:QQ 把消息123设为群精华] 稍等");
        assert_eq!(clean, "好的,我来处理。 稍等");
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].0, AgentMode::QqOps);
        assert_eq!(ops[0].1, "把消息123设为群精华");

        let (clean, ops) = parse_task_ops("[任务:沙箱 下载图片并处理]正文[任务:QQ 打卡]");
        assert_eq!(clean, "正文");
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].0, AgentMode::Sandbox);
        assert_eq!(ops[1].0, AgentMode::QqOps);

        // 空目标 / 未知模式:原样保留
        let (clean, ops) = parse_task_ops("x[任务:QQ ]y");
        assert_eq!(clean, "x[任务:QQ ]y");
        assert!(ops.is_empty());
        let (clean, ops) = parse_task_ops("x[任务:别的 目标]y");
        assert_eq!(clean, "x[任务:别的 目标]y");
        assert!(ops.is_empty());
        // 未闭合:原样保留
        let (clean, ops) = parse_task_ops("x[任务:QQ 未闭合");
        assert_eq!(clean, "x[任务:QQ 未闭合");
        assert!(ops.is_empty());
    }

    #[test]
    fn json_extraction() {
        assert_eq!(extract_json("好的 {\"a\":1} 结束").unwrap()["a"], 1);
        assert_eq!(extract_json("```json\n{\"tools\":[\"a\"]}\n```").unwrap()["tools"][0], "a");
        assert!(extract_json("没有 json").is_none());
    }

    #[test]
    fn select_parse() {
        let (tools, plan) = parse_select("{\"tools\":[\"set_essence_msg\",\"get_msg\"],\"plan\":\"先查再设\"}").unwrap();
        assert_eq!(tools.len(), 2);
        assert_eq!(plan, "先查再设");
        let (tools, _) = parse_select("{\"tools\":[],\"plan\":\"无法\"}").unwrap();
        assert!(tools.is_empty());
        assert!(parse_select("not json").is_err());
    }

    #[test]
    fn step_parse() {
        match parse_step("{\"tool\":\"set_essence_msg\",\"params\":{\"message_id\":123},\"note\":\"设置\"}") {
            StepOutput::Call { tool, params, note } => {
                assert_eq!(tool, "set_essence_msg");
                assert_eq!(params["message_id"], 123);
                assert_eq!(note, "设置");
            }
            _ => panic!("应为工具调用"),
        }
        match parse_step("{\"done\":true,\"summary\":\"搞定\"}") {
            StepOutput::Done { summary } => assert_eq!(summary, "搞定"),
            _ => panic!("应为完成"),
        }
        assert!(matches!(parse_step("乱输出"), StepOutput::Invalid(_)));
    }

    #[test]
    fn sensitive_filter() {
        assert!(AgentManager::sensitive_allowed("owner"));
        assert!(AgentManager::sensitive_allowed("admin"));
        assert!(AgentManager::sensitive_allowed("trusted"));
        assert!(!AgentManager::sensitive_allowed("member"));
        assert!(!AgentManager::sensitive_allowed("private"));
    }

    #[test]
    fn catalogs_complete() {
        for mode in [AgentMode::QqOps, AgentMode::Sandbox] {
            let mut names = Vec::new();
            for t in catalog(mode) {
                assert!(!names.contains(&t.name), "重复工具: {}", t.name);
                names.push(t.name);
            }
            assert!(tool_by_name(mode, names[0]).is_some());
            assert!(tool_by_name(mode, "nonexistent").is_none());
        }
        // 用户点名的基础操作都在 QQ 目录里
        for need in ["set_essence_msg", "set_friend_remark", "set_group_card", "send_group_sign"] {
            assert!(tool_by_name(AgentMode::QqOps, need).is_some(), "缺少 {need}");
        }
    }

    #[test]
    fn select_sys_hides_sensitive_for_member() {
        let s = select_sys(AgentMode::QqOps, "member");
        assert!(!s.contains("delete_msg"));
        assert!(s.contains("set_essence_msg"));
        let s2 = select_sys(AgentMode::QqOps, "owner");
        assert!(s2.contains("delete_msg"));
    }
}
