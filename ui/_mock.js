// [开发临时文件] 浏览器预览用的 mock 后端 —— 模拟 Tauri invoke,提交前删除。
// 用法:浏览器直接打开 ui/index.html 即可带数据预览。
// 只在 Tauri 未注入时生效(if (!window.__TAURI__) 保护):真实桌面环境走 Tauri,本文件不接管、不影响。
if (!window.__TAURI__) (function () {
  const now = Math.floor(Date.now() / 1000);
  const mockConfig = {
    napcat: {
      mode: "forward", ws_url: "ws://127.0.0.1:3001", reverse_port: 3005,
      access_token: "", self_id: "", group_trigger: "at_or_keyword", keyword: "小灯",
      reply_quoted: true, reply_pending: true, pending_delay_secs: 15,
      pending_text: "让我想想……", max_msg_len: 1800, segment_delay_ms: 300,
    },
    chat: {
      enable_group: true, enable_private: true, decider: false,
      context_tokens: 65536, reserve_tokens: 1024, history_target_tokens: 0,
      summarize: true, summarize_tokens: 600, clean_after_hours: 24, estimate_ratio: 1.15,
      ignore_prefix_enabled: true, ignore_prefix: "*",
      recent_max_tokens: 3000, recent_keep_msgs: 10,
      trail: { enabled: true, inject_mode: "window", window_minutes: 5, max_entries: 10, max_tokens: 800 },
      interject: {
        enabled: true, mode: "adaptive", cooldown_messages: 25, rate_every_messages: 5,
        full_context: false, base_probability: 0.05, interject_max_tokens: 120,
        activity_window_minutes: 2, soft_at_reply: true, names: "小灯", hooks: "为什么,怎么",
      },
      memory: {
        enabled: true, placement: "back", auto_placement: true, auto_cooldown_minutes: 120,
        max_entries: 30, max_entry_chars: 200, max_tokens: 1200,
      },
    },
    models: [
      { name: "DeepSeek V4 Flash", kind: "chat", base_url: "https://api.deepseek.com", api_key: "sk-***", model: "deepseek-v4-flash", thinking: "auto", reasoning_effort: "high", temperature: 1.0, max_tokens: 8192, timeout_secs: 120, price_cache_hit: 0.02, price_input: 1.0, price_output: 2.0 },
      { name: "DeepSeek V4 Pro", kind: "reasoner", base_url: "https://api.deepseek.com", api_key: "sk-***", model: "deepseek-v4-pro", thinking: "enabled", reasoning_effort: "high", temperature: 1.0, max_tokens: 8192, timeout_secs: 180, price_cache_hit: 0.025, price_input: 3.0, price_output: 6.0 },
    ],
    prompts: [
      { id: "light", name: "小灯", prompt: "你是群友小灯,一个机智友好的 QQ 群机器人。说话简短自然,偶尔用颜文字。" },
      { id: "assistant", name: "通用助手", prompt: "你是一个乐于助人的 AI 助手。" },
    ],
    cost: { wallet_balance: 32.15 },
    ui_refresh_ms: 500,
    active_model: "DeepSeek V4 Flash",
    active_prompt: "light",
  };

  const sessions = [
    { key: "g123456789", status: "replying", count: 24, tokens: 18000, has_summary: true, paused: false },
    { key: "g987654321", status: "thinking", count: 156, tokens: 92000, has_summary: true, paused: false },
    { key: "p10086", status: "idle", count: 5, tokens: 1200, has_summary: false, paused: false },
  ];

  const memories = [
    { key: "g123456789", entries: [
      { index: 1, source: "model", date: "08-20", text: "群友小张喜欢原神,经常凌晨三点还在聊抽卡" },
      { index: 2, source: "user", date: "08-20", text: "群里周一到周五晚上比较活跃" },
    ] },
    { key: "p10086", entries: [
      { index: 1, source: "model", date: "08-19", text: "用户在准备考研,喜欢简短的回答" },
    ] },
  ];

  const costOverview = {
    today: { prompt: 820000, completion: 12000, cache_hit: 790000, cache_miss: 30000, cost: 0.1234 },
    remaining: 31.9666, wallet_balance: 32.09,
    by_category: [
      ["dialogue", { cache_hit: 700000, cache_miss: 20000, completion: 9000, calls: 42, cost: 0.09 }],
      ["decide", { cache_hit: 60000, cache_miss: 6000, completion: 1500, calls: 18, cost: 0.021 }],
      ["summarize", { cache_hit: 20000, cache_miss: 3000, completion: 1200, calls: 2, cost: 0.012 }],
      ["interject", { cache_hit: 10000, cache_miss: 1000, completion: 300, calls: 9, cost: 0.0004 }],
    ],
  };

  const sessionDetail = {
    status: "idle", count: 24, tokens: 18000, has_summary: true,
    summary: "前情提要:群里讨论了周末聚餐地点,小张推荐了新开的火锅店,大家决定周六晚上去。",
    live: null,
    events: [
      { type: "msg_in", turn: 1, ts: now - 3600, id: "m1", trigger: "at", text: "@小灯 今天天气怎么样,适合出去玩吗?" },
      { type: "think", turn: 1, ts: now - 3590, tokens: 850, text: "用户询问天气。我没有实时天气数据,但可以给出建议性回答,提醒查天气预报,顺便推荐适合的出行方式。" },
      { type: "msg_out", turn: 1, ts: now - 3580, id: "m2", model: "deepseek-v4-flash", text: "今天天气不错哦~ 我没法看实时天气预报,建议出门前查一下。如果天气好,周末去公园野餐也是个不错的选择呢 (◕‿◕)", usage: { prompt_tokens: 12000, cache_hit: 11000, cache_miss: 1000, completion_tokens: 300, reasoning_tokens: 850 } },
      { type: "msg_in", turn: 2, ts: now - 1800, id: "m3", trigger: "keyword", text: "小灯小灯,今晚吃什么" },
      { type: "decide", turn: 2, ts: now - 1795, verdict: false, model: "deepseek-v4-flash", ms: 420, text: "用户在问晚饭吃什么,属于闲聊水群,当前群里对话已经结束这个话题,无需回复。" },
      { type: "cmd", turn: 3, ts: now - 900, text: "/stats", reply: "本会话共 24 条消息,约 18000 tokens。当前模型:deepseek-v4-flash,人设:小灯。" },
      { type: "fold", turn: 4, ts: now - 600, folded: 12, summary_tokens: 600 },
      { type: "lite_out", turn: 5, ts: now - 300, model: "deepseek-v4-flash", text: "你们聊得好热闹,火锅店听起来不错啊,记得给我留个位置(并没有嘴)" },
      { type: "error", turn: 6, ts: now - 120, text: "模型请求超时(120s),本回合已中止" },
    ],
  };

  let seq = 0;
  const mk = (event) => ({ seq: ++seq, event });
  const startupEvents = [
    mk({ type: "log", level: "info", msg: "配置已加载,共 2 个模型 / 2 个人设" }),
    mk({ type: "log", level: "info", msg: "正向 WS 已连接: ws://127.0.0.1:3001" }),
    mk({ type: "notice", notice_type: "startup", desc: "机器人已登录,QQ 123456789" }),
    mk({ type: "msg_in", key: "g123456789", text: "@小灯 今天天气怎么样" }),
    mk({ type: "msg_out", key: "g123456789", text: "今天天气不错哦~" }),
    mk({ type: "log", level: "warn", msg: "会话 g987654321 上下文接近预算,已自动折叠" }),
    mk({ type: "log", level: "stats", msg: "📊 deepseek-v4-flash prompt=12000t 生成=300t 缓存命中=11000t(92%) 未命中=1000t 1420ms" }),
    mk({ type: "log", level: "error", msg: "模型 DeepSeek V4 Pro 测试失败: 401 Unauthorized" }),
  ];
  let eventsSent = false;

  window.__TAURI__ = {
    core: {
      invoke(cmd, args) {
        return new Promise((resolve, reject) => {
          setTimeout(() => {
            switch (cmd) {
              case "get_config": resolve(JSON.parse(JSON.stringify(mockConfig))); break;
              case "get_status_view": resolve({ running: true, paused: false, connected: true, mode: "forward", endpoint: "ws://127.0.0.1:3001", self_id: 123456789, last_error: "" }); break;
              case "get_sessions": resolve(JSON.parse(JSON.stringify(sessions))); break;
              case "get_all_memories": resolve(JSON.parse(JSON.stringify(memories))); break;
              case "get_cost_overview": resolve(JSON.parse(JSON.stringify(costOverview))); break;
              case "get_events": {
                if (!eventsSent) { eventsSent = true; resolve({ events: startupEvents, latest_seq: seq }); }
                else resolve({ events: [], latest_seq: seq });
                break;
              }
              case "get_session_detail": resolve(JSON.parse(JSON.stringify(sessionDetail))); break;
              case "get_placement_proposal": resolve(null); break;
              case "plugin:opener|open_url": reject(new Error("预览环境不支持")); break;
              default: resolve(null);
            }
          }, 30);
        });
      },
    },
  };
})();
