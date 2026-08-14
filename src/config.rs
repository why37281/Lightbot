//! 配置模型:默认值 + JSON 读写。
//! 所有配置均可通过 GUI 修改,配置文件仅作持久化载体。

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tauri::Manager;

// ---------- 配置结构 ----------

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    /// NapCat 连接配置
    pub napcat: NapcatConfig,
    /// 模型列表(OpenAI 兼容,可配置多个)
    pub models: Vec<ModelConfig>,
    /// 当前激活模型(按 name 匹配)
    pub active_model: String,
    /// 对话行为配置
    pub chat: ChatConfig,
    /// 人设预设列表
    pub prompts: Vec<PromptPreset>,
    /// 当前激活人设
    pub active_prompt: String,
    /// 费用与钱包配置(价格随 DeepSeek 官方调价更新,由用户在 GUI 填写)
    pub cost: CostConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            napcat: NapcatConfig::default(),
            models: vec![
                ModelConfig::deepseek_v4_flash(),
                ModelConfig::deepseek_v4_pro(),
            ],
            active_model: "deepseek-v4-flash".into(),
            chat: ChatConfig::default(),
            prompts: vec![PromptPreset::default()],
            active_prompt: "default".into(),
            cost: CostConfig::default(),
        }
    }
}

/// 费用与钱包:价格随官方调价波动,由用户在 GUI 维护真实值。
/// 用量按天落盘时记录「当时价格」,历史费用不会因改价而漂移。
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct CostConfig {
    /// 钱包余额(元,用户填写,用于展示剩余额度)
    pub wallet_balance: f64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self { wallet_balance: 0.0 }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct NapcatConfig {
    /// 连接模式: "forward" 正向 WS(机器人主动连 NapCat) / "reverse" 反向 WS(NapCat 连我们)
    pub mode: String,
    /// 正向 WS 地址,如 ws://127.0.0.1:3001
    pub ws_url: String,
    /// 反向 WS 监听端口
    pub reverse_port: u16,
    /// NapCat 的 access_token(未设置则留空)
    pub access_token: String,
    /// 机器人 QQ 号(留空则连接后自动获取)
    pub self_id: String,
    /// 群聊触发方式: "at" / "keyword" / "at_or_keyword"
    pub group_trigger: String,
    /// 关键词触发的前缀或包含词(trigger 含 keyword 时生效)
    pub keyword: String,
    /// 是否响应「回复机器人消息」的引用
    pub reply_quoted: bool,
    /// 思考/等待时是否发提示消息(延迟到思考超过阈值才发)
    pub reply_pending: bool,
    /// 思考提示延迟(秒):思考超过该时长仍未完成才提示;
    /// 首 token 到达前的等待(约 3-5s)不计入,实现按 4s 估计扣除
    pub pending_delay_secs: u64,
    /// 提示消息文本
    pub pending_text: String,
    /// 单条消息最大字符数,超出自动分段
    pub max_msg_len: usize,
    /// 两条分段消息之间的发送间隔(毫秒)
    pub segment_delay_ms: u64,
}

impl Default for NapcatConfig {
    fn default() -> Self {
        Self {
            mode: "forward".into(),
            ws_url: "ws://127.0.0.1:3001".into(),
            reverse_port: 3005,
            access_token: String::new(),
            self_id: String::new(),
            group_trigger: "at_or_keyword".into(),
            keyword: String::new(),
            reply_quoted: true,
            reply_pending: true,
            pending_delay_secs: 15,
            pending_text: "正在思考,请稍候…".into(),
            max_msg_len: 1800,
            segment_delay_ms: 300,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct ModelConfig {
    /// 显示名称(用于 GUI 选择与 /model 命令)
    pub name: String,
    /// OpenAI 兼容 API 地址
    pub base_url: String,
    pub api_key: String,
    /// 实际模型名,如 deepseek-v4-flash / deepseek-v4-pro / gpt-4o-mini
    pub model: String,
    /// 兼容旧配置的类型字段(不再用于请求构造,保留以便旧配置迁移)
    pub kind: String,
    /// 思考模式: "auto"(不显式设置,遵循模型默认)/ "enabled" / "disabled"
    /// (DeepSeek V4 通过 thinking 参数切换,非 DeepSeek 模型自动忽略)
    pub thinking: String,
    /// 推理强度: low / high / max(DeepSeek 思考模式下生效,默认 high)
    pub reasoning_effort: String,
    pub temperature: f64,
    pub max_tokens: u32,
    pub timeout_secs: u64,
    /// 输入价格(元 / 1M tokens,未命中缓存)。DeepSeek V4 官方价:flash 1 元 / pro 3 元
    pub price_input: f64,
    /// 输入价格(元 / 1M tokens,命中缓存)。DeepSeek V4 官方价:flash 0.02 元 / pro 0.025 元
    pub price_cache_hit: f64,
    /// 输出价格(元 / 1M tokens)。DeepSeek V4 官方价:flash 2 元 / pro 6 元
    pub price_output: f64,
}

impl ModelConfig {
    fn deepseek_v4_flash() -> Self {
        Self {
            name: "deepseek-v4-flash".into(),
            base_url: "https://api.deepseek.com".into(),
            api_key: String::new(),
            model: "deepseek-v4-flash".into(),
            kind: "chat".into(),
            thinking: "auto".into(),
            reasoning_effort: "high".into(),
            temperature: 1.0,
            max_tokens: 8192,
            timeout_secs: 120,
            price_input: 1.0,
            price_cache_hit: 0.02,
            price_output: 2.0,
        }
    }
    fn deepseek_v4_pro() -> Self {
        Self {
            name: "deepseek-v4-pro".into(),
            base_url: "https://api.deepseek.com".into(),
            api_key: String::new(),
            model: "deepseek-v4-pro".into(),
            kind: "chat".into(),
            thinking: "auto".into(),
            reasoning_effort: "high".into(),
            temperature: 1.0,
            max_tokens: 8192,
            timeout_secs: 180,
            price_input: 3.0,
            price_cache_hit: 0.025,
            price_output: 6.0,
        }
    }
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self::deepseek_v4_flash()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct ChatConfig {
    /// 单会话上下文预算(token):system + 摘要 + 历史 + 当前提问 的总上限
    pub context_tokens: u32,
    /// 回复预留 token(从上下文预算中扣除,保证生成空间)
    pub reserve_tokens: u32,
    /// 是否启用群聊
    pub enable_group: bool,
    /// 是否启用私聊
    pub enable_private: bool,
    /// 超出预算时是否用 LLM 折叠旧消息为摘要(缓存友好:摘要固定在前缀)
    pub summarize: bool,
    /// 摘要最大 token
    pub summarize_tokens: u32,
    /// 空闲会话内存清理(小时,0 = 不清理;会话文件仍保留)
    pub clean_after_hours: u64,
    /// token 估算保守系数(防止低估导致超限)
    pub estimate_ratio: f64,
    /// 回复决策器:任何触发命中后,先由当前模型判断是否需要回复(默认关闭;
    /// 每次触发增加一次轻量模型调用)
    pub decider: bool,
    /// 近期历史主动折叠阈值(token):历史超过该值就折叠进摘要(0 = 仅在超总预算时折叠)
    /// 仅方案一(记忆在历史后)使用。
    pub history_target_tokens: u32,
    /// 忽略 * 前缀消息:QQ 消息以 * 开头时,模型不回复、不记入历史(给群友留自由交流空间)
    pub ignore_star: bool,
    /// 方案二(摘要→记忆→新历史)的新历史折叠阈值(token):
    /// 新历史超过该值即把超出「保留条数」的部分摘要进旧历史。
    pub recent_max_tokens: u32,
    /// 方案二折叠时至少保留的最近消息条数(超出部分折叠进摘要)
    pub recent_keep_msgs: u32,
    /// 长期记忆系统配置
    pub memory: MemoryConfig,
    /// 群聊轨迹(未触发消息缓冲)配置
    pub trail: TrailConfig,
    /// 主动插话(活人感)配置
    pub interject: InterjectConfig,
}

impl Default for ChatConfig {
    fn default() -> Self {
        Self {
            context_tokens: 65536,
            reserve_tokens: 1024,
            enable_group: true,
            enable_private: true,
            summarize: true,
            summarize_tokens: 600,
            clean_after_hours: 24,
            estimate_ratio: 1.15,
            decider: false,
            history_target_tokens: 8192,
            ignore_star: true,
            recent_max_tokens: 3000,
            recent_keep_msgs: 10,
            memory: MemoryConfig::default(),
            trail: TrailConfig::default(),
            interject: InterjectConfig::default(),
        }
    }
}

/// 长期记忆配置
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct MemoryConfig {
    /// 总开关:开启后模型可通过回复末尾标记自动写入记忆
    pub enabled: bool,
    /// 记忆条数上限(超出删最旧)
    pub max_entries: u32,
    /// 单条记忆最大字符数
    pub max_entry_chars: u32,
    /// 记忆总 token 上限(超出删最旧;保护上下文预算)
    pub max_tokens: u32,
    /// 记忆消息位置(两种缓存策略,详见 README「记忆位置策略」):
    ///  "back"  方案一:历史→记忆→提问 —— 记忆每次随历史增长而 miss,适合记忆少/变化频繁;
    ///  "front" 方案二:摘要→记忆→新历史→提问 —— 记忆稳定命中缓存,适合记忆多/变化少,
    ///          新历史超过 recent_max_tokens 时折叠进摘要。
    pub placement: String,
    /// 自动控制:开启后程序按成本模型持续评估两方案,需要切换时弹出醒目审批弹窗,
    /// 用户批准后才真正切换(带滞回与冷却,避免在分界线附近反复横跳)。
    pub auto_placement: bool,
    /// 切换/拒绝提案后的冷却时长(分钟,默认 120 = 2 小时);冷却期内不再评估切换
    pub auto_cooldown_minutes: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 30,
            max_entry_chars: 200,
            max_tokens: 1200,
            placement: "front".into(),
            auto_placement: true,
            auto_cooldown_minutes: 120,
        }
    }
}

/// 群聊轨迹配置:未触发对话的普通消息短期缓冲,
/// 触发对话时拼在提问前,让机器人知道群里刚说了什么(解决"鱼的记忆")。
/// 轨迹消息位于记忆之后、提问之前,变化只影响自身与提问,不影响历史/记忆前缀。
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct TrailConfig {
    /// 总开关
    pub enabled: bool,
    /// 上下文注入行为:
    ///  "window"         窗口注入(默认,即现状):所有完整对话都注入,内容按下面三个参数过滤;
    ///  "all"            全部注入:无论触发方式都注入,不设任何上限(时间窗口/条数/token 全失效,
    ///                   缓冲保留 24 小时,整段缓冲全部注入);
    ///  "triggered_only" 不额外注入:仅当消息 @ 或引用机器人时才注入轨迹。
    pub inject_mode: String,
    /// 轨迹窗口(分钟),窗口外的消息过期(仅 window 模式生效;其他模式保留 24 小时)
    pub window_minutes: u64,
    /// 最多保留条数(超出丢最旧;all 模式不设上限)
    pub max_entries: u32,
    /// 轨迹总 token 上限(超出丢最旧,保护成本;all 模式不设上限)
    pub max_tokens: u32,
}

impl Default for TrailConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            inject_mode: "window".into(),
            window_minutes: 5,
            max_entries: 10,
            max_tokens: 800,
        }
    }
}

/// 主动插话配置:概率采样 + 软 at,让机器人在群里"像活人一样"偶尔接话。
/// 插话走轻量通道(单轮、不落盘、不占用会话上下文),软 at 走完整通道(必回)。
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct InterjectConfig {
    /// 总开关
    pub enabled: bool,
    /// 模式: "adaptive" 按群活跃度自适应 / "fixed" 固定概率
    pub mode: String,
    /// 插话冷却:距上次主动发言以来,群里新消息达到该条数才允许再次插话
    /// (比固定时间更符合群聊节奏:活跃群攒得快,冷清群自然少插话)
    pub cooldown_messages: u32,
    /// 基线概率(0.0~1.0),命中钩子词加分、纯水消息减分
    pub base_probability: f64,
    /// 插话输出上限 tokens(轻量通道,保持低成本)
    pub interject_max_tokens: u32,
    /// 活跃度统计窗口(分钟):计算消息速率的滑动窗口跨度
    pub activity_window_minutes: u64,
    /// 消息中提到机器人称呼时必回一次(软 at,完整通道,刷新插话冷却)
    pub soft_at_reply: bool,
    /// 机器人称呼列表(逗号分隔),如 "小灯,灯宝"
    pub names: String,
    /// 钩子词列表(逗号分隔),命中会提升插话概率
    pub hooks: String,
}

impl Default for InterjectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: "adaptive".into(),
            cooldown_messages: 25,
            base_probability: 0.05,
            interject_max_tokens: 120,
            activity_window_minutes: 2,
            soft_at_reply: true,
            names: "小灯".into(),
            hooks: "怎么,为什么,帮,大家觉得,你们说".into(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(default)]
pub struct PromptPreset {
    pub id: String,
    pub name: String,
    pub prompt: String,
}

impl Default for PromptPreset {
    fn default() -> Self {
        Self {
            id: "default".into(),
            name: "默认助手".into(),
            prompt: "你是一个乐于助人的 AI 助手,通过 QQ 与用户对话。\n\
回复要求:\n\
- 简洁、自然、口语化,不要长篇大论;\n\
- 除非被明确要求,不要输出 Markdown 表格、代码块等格式;\n\
- 不确定的事情要如实说明。".into(),
        }
    }
}

impl Config {
    /// 查找当前激活模型
    pub fn active_model(&self) -> Option<&ModelConfig> {
        self.models
            .iter()
            .find(|m| m.name == self.active_model)
            .or_else(|| self.models.first())
    }

    pub fn prompt(&self) -> Option<&PromptPreset> {
        self.prompts
            .iter()
            .find(|p| p.id == self.active_prompt)
            .or_else(|| self.prompts.first())
    }
}

// ---------- 路径与读写 ----------

pub fn config_path(app: &tauri::AppHandle) -> PathBuf {
    let dir = app
        .path()
        .app_config_dir()
        .unwrap_or_else(|_| PathBuf::from("."));
    dir.join("config.json")
}

pub fn sessions_dir(app: &tauri::AppHandle) -> PathBuf {
    let dir = app
        .path()
        .app_data_dir()
        .unwrap_or_else(|_| PathBuf::from("."));
    dir.join("sessions")
}

pub fn load_config(path: &Path) -> Config {
    match fs::read_to_string(path) {
        Ok(s) => match serde_json::from_str::<Config>(&s) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[config] 解析失败({e}),使用默认配置");
                Config::default()
            }
        },
        Err(_) => Config::default(),
    }
}

pub fn save_config(path: &Path, cfg: &Config) -> Result<(), String> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let s = serde_json::to_string_pretty(cfg).map_err(|e| e.to_string())?;
    // 先写临时文件再改名,避免写坏
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, s).map_err(|e| e.to_string())?;
    fs::rename(&tmp, path).map_err(|e| e.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_roundtrip() {
        let cfg = Config::default();
        let s = serde_json::to_string_pretty(&cfg).unwrap();
        let back: Config = serde_json::from_str(&s).unwrap();
        assert_eq!(back.active_model, "deepseek-v4-flash");
        assert_eq!(back.models.len(), 2);
        assert_eq!(back.active_model().unwrap().model, "deepseek-v4-flash");
        // 新字段(thinking / reasoning_effort)默认值
        assert_eq!(back.models[0].thinking, "auto");
        assert_eq!(back.models[0].reasoning_effort, "high");
        assert_eq!(back.models[0].base_url, "https://api.deepseek.com");
    }

    #[test]
    fn config_missing_fields_default() {
        // 旧配置缺少新字段时应能正常解析(serde default)
        let s = r#"{"napcat":{"ws_url":"ws://x:1"},"models":[],"chat":{},"prompts":[]}"#;
        let cfg: Config = serde_json::from_str(s).unwrap();
        assert_eq!(cfg.napcat.reverse_port, 3005);
        assert_eq!(cfg.chat.context_tokens, 65536);
    }
}
