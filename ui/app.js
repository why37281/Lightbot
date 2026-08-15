// LightBot 前端逻辑(零构建,Tauri v2 withGlobalTauri)
//
// 结构:
//   1. 工具与全局状态
//   2. 主题 / 面板切换 / 日志
//   3. 事件总线(后端 frontend 事件 → 视图)
//   4. 配置表单绑定与保存 / 模型 / 人设
//   5. 会话列表(状态胶囊)与记忆管理
//   6. 总览(缓存统计 + 今日开销面板)
//   7. 会话详情页(时间线 / 流式直播 / 编辑删除 / 停止)
//   8. 记忆位置切换审批弹窗

const T = window.__TAURI__;
const invoke = T ? T.core.invoke : async () => { throw new Error("请在 Tauri 桌面环境中运行"); };

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

// ---------- 1. 全局状态 ----------
let cfg = null;            // 当前配置副本
let running = false;       // 机器人运行中
let stats = { hit: 0, miss: 0, last: null }; // 本次运行缓存统计累计
const logs = [];           // 日志行 {ts, level, msg}

function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ---------- 2. 主题 ----------
const THEME_KEY = "lightbot-theme";

function applyTheme(t) {
  document.body.dataset.theme = t;
  $("#btn-theme").textContent = t === "light" ? "🌙" : "☀️";
  try { localStorage.setItem(THEME_KEY, t); } catch (e) { /* 忽略 */ }
}

$("#btn-theme").addEventListener("click", () => {
  const cur = document.body.dataset.theme === "light" ? "dark" : "light";
  applyTheme(cur);
});

(function initTheme() {
  let t = null;
  try { t = localStorage.getItem(THEME_KEY); } catch (e) { /* 忽略 */ }
  if (!t) {
    t = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches
      ? "light" : "dark";
  }
  applyTheme(t);
})();

// ---------- 2b. 面板切换 ----------
function switchTab(name) {
  $$("#sidebar nav a").forEach((x) => x.classList.toggle("active", x.dataset.tab === name));
  $$(".tab").forEach((t) => t.classList.toggle("active", t.id === "tab-" + name));
  if (name === "chat") {
    refreshSessions();
    renderMemories();
  } else if (name === "overview") {
    refreshCost();
  }
}

$$("#sidebar nav a").forEach((a) => {
  a.addEventListener("click", () => switchTab(a.dataset.tab));
});

// ---------- 2c. 日志 ----------
function addLog(level, msg) {
  const now = new Date();
  const ts = now.toTimeString().slice(0, 8);
  logs.push({ ts, level, msg });
  if (logs.length > 2000) logs.shift();
  renderLogs();
}

function renderLogs() {
  const lv = $("#log-level").value;
  const box = $("#log-box");
  box.innerHTML = "";
  let shown = 0;
  for (const l of logs) {
    if (lv !== "all" && l.level !== lv) continue;
    shown++;
    const div = document.createElement("div");
    div.className = "log-line " + l.level;
    div.innerHTML = `<span class="t">${l.ts}</span>${escapeHtml(l.msg)}`;
    box.appendChild(div);
  }
  $("#log-count").textContent = `共 ${logs.length} 条`;
  box.scrollTop = box.scrollHeight;
}

$("#log-level").addEventListener("change", renderLogs);
$("#btn-clear-log").addEventListener("click", () => { logs.length = 0; renderLogs(); });

// ---------- 3. 事件总线(拉模式) ----------
// 事件走 get_events 轮询拉取(推送链路在部分环境下不可用,invoke 已被证明可靠)。
// 轮询间隔可配置(cfg.ui_refresh_ms,默认 500ms)。
let lastEventSeq = 0;

async function fetchEvents() {
  try {
    const r = await invoke("get_events", { afterSeq: lastEventSeq });
    for (const item of (r.events || [])) {
      if (item.seq > lastEventSeq) lastEventSeq = item.seq;
      if (item.event) handleEvent(item.event);
    }
    // 事件被环形缓冲淘汰时,以服务端最新序号对齐,避免反复拉取旧区间
    if (r.latest_seq != null && r.latest_seq > lastEventSeq) lastEventSeq = r.latest_seq;
  } catch (e) { /* 忽略 */ }
}

let eventPollTimer = null;
function startEventPoller() {
  if (eventPollTimer) clearInterval(eventPollTimer);
  // 无最小值限制(仅要求为正数,否则回落默认 500);保留 5000 上限防误填
  const raw = parseInt(cfg?.ui_refresh_ms);
  const ms = Math.min(5000, raw > 0 ? raw : 500);
  eventPollTimer = setInterval(fetchEvents, ms);
  fetchEvents(); // 立即拉一次
}

function handleEvent(ev) {
  switch (ev.type) {
    case "log": addLog(ev.level, ev.msg); break;
    case "notice": addLog("notice", `🔔 [${ev.notice_type}] ${ev.desc}`); break;
    case "msg_in": addLog("msg_in", `← ${ev.key}: ${ev.text}`); break;
    case "msg_out": addLog("msg_out", `→ ${ev.key}: ${ev.text}`); break;
    case "llm_stats": {
      const hitRatio = ev.cache_hit + ev.cache_miss > 0
        ? Math.round((ev.cache_hit / (ev.cache_hit + ev.cache_miss)) * 100) : 0;
      stats.hit += ev.cache_hit || 0;
      stats.miss += ev.cache_miss || 0;
      stats.last = { ...ev, hitRatio };
      addLog("stats",
        `📊 ${ev.model} prompt=${ev.prompt_tokens}t 生成=${ev.completion_tokens}t ` +
        `缓存命中=${ev.cache_hit}t(${hitRatio}%) 未命中=${ev.cache_miss}t` +
        `${ev.reasoning_tokens ? ` 思考=${ev.reasoning_tokens}t` : ""} ${ev.elapsed_ms}ms`);
      renderOverview();
      scheduleCostRefresh();
      break;
    }
    case "session_changed":
      refreshSessions();
      // 详情页打开时同步条数/tokens
      if (detailState.key === ev.key) {
        $("#detail-meta").textContent = `${ev.count} 条消息 · 约 ${(ev.tokens || 0).toLocaleString()} tokens`;
      }
      break;
    case "status": updateStatusView(ev.status); break;
    case "session_status": {
      updateSessionStatusCell(ev.key, ev.status);
      if (detailState.key === ev.key) setDetailStatus(ev.status);
      break;
    }
    case "trace": handleTraceEvent(ev); break;
    case "turn_delta": handleTurnDelta(ev); break;
    case "placement_proposal": showApprovalModal(ev.proposal); break;
  }
}

// ---------- 状态显示 ----------
function updateStatusView(st) {
  const dot = $("#st-dot");
  const txt = $("#st-text");
  if (!running) { dot.className = "dot gray"; txt.textContent = "未启动"; }
  else if (st.paused) { dot.className = "dot yellow"; txt.textContent = "已停止回复 · 仅接收消息"; }
  else if (st.connected) { dot.className = "dot green"; txt.textContent = "已连接 NapCat"; }
  else { dot.className = "dot red"; txt.textContent = "未连接"; }

  $("#ov-running").textContent = running ? "运行中" : "未启动";
  $("#ov-conn").textContent = running ? (st.connected ? "已连接" : `未连接${st.last_error ? " · " + st.last_error : ""}`) : "-";
  $("#ov-mode").textContent = st.mode === "reverse" ? "反向 WS" : "正向 WS";
  $("#ov-endpoint").textContent = st.endpoint || "-";
  $("#ov-self").textContent = st.self_id ? String(st.self_id) : (cfg?.napcat?.self_id || "自动获取中");
}

function updatePausedUi() {
  const btn = $("#btn-detail-stop");
  if (btn) btn.textContent = paused ? "▶ 恢复" : "■ 停止";
  updateStatusView({ connected: true, mode: "", endpoint: "", self_id: null, last_error: "", paused });
}

function renderOverview() {
  const total = stats.hit + stats.miss;
  $("#ov-hit-ratio").textContent = total > 0 ? Math.round((stats.hit / total) * 100) + "%" : "-";
  $("#ov-hit").textContent = stats.hit > 0 ? stats.hit.toLocaleString() + " tokens" : "-";
  $("#ov-miss").textContent = stats.miss > 0 ? stats.miss.toLocaleString() + " tokens" : "-";
  $("#ov-last-ms").textContent = stats.last ? `${stats.last.elapsed_ms} ms` : "-";
}

// ---------- 4. 配置读取与表单绑定 ----------
async function loadConfig() {
  cfg = await invoke("get_config");
  bindConfigToForm();
  renderModels();
  renderPrompts();
  renderOverview();
  const sv = await invoke("get_status_view");
  running = sv.running;
  paused = !!sv.paused;
  updateStatusView({ connected: sv.connected, mode: sv.mode, endpoint: sv.endpoint, self_id: sv.self_id, last_error: sv.last_error, paused });
  updatePausedUi();
  $("#btn-toggle").textContent = running ? "停止" : "启动";
  await refreshSessions();
  await renderMemories();
  await refreshCost();
  startEventPoller();
  // 兜底:启动时若有未处理的切换提案(事件可能已错过),弹窗提醒
  try {
    const p = await invoke("get_placement_proposal");
    if (p) showApprovalModal(p);
  } catch (e) { /* 忽略 */ }
}

function bindConfigToForm() {
  const n = cfg.napcat, c = cfg.chat, ij = c.interject, mem = c.memory;
  $("#cfg-mode").value = n.mode;
  $("#cfg-ws-url").value = n.ws_url;
  $("#cfg-reverse-port").value = n.reverse_port;
  $("#cfg-token").value = n.access_token;
  $("#cfg-self").value = n.self_id;
  $("#cfg-trigger").value = n.group_trigger;
  $("#cfg-keyword").value = n.keyword;
  $("#cfg-reply-quoted").checked = n.reply_quoted;
  $("#cfg-reply-pending").checked = n.reply_pending;
  $("#cfg-pending-delay").value = n.pending_delay_secs;
  $("#cfg-pending-text").value = n.pending_text;
  $("#cfg-max-len").value = n.max_msg_len;
  $("#cfg-seg-delay").value = n.segment_delay_ms;
  $("#cfg-enable-group").checked = c.enable_group;
  $("#cfg-enable-private").checked = c.enable_private;
  $("#cfg-decider").checked = c.decider;
  $("#cfg-context").value = c.context_tokens;
  $("#cfg-reserve").value = c.reserve_tokens;
  $("#cfg-history-target").value = c.history_target_tokens;
  $("#cfg-summarize").checked = c.summarize;
  $("#cfg-summarize-tokens").value = c.summarize_tokens;
  $("#cfg-clean-hours").value = c.clean_after_hours;
  $("#cfg-est-ratio").value = c.estimate_ratio;
  $("#cfg-ignore-star").checked = c.ignore_prefix_enabled;
  $("#cfg-ignore-prefix").value = c.ignore_prefix;
  $("#cfg-wallet").value = cfg.cost.wallet_balance;
  $("#cfg-ui-refresh").value = cfg.ui_refresh_ms;
  $("#cfg-interject").checked = ij.enabled;
  $("#cfg-interject-mode").value = ij.mode;
  $("#cfg-interject-cooldown").value = ij.cooldown_messages;
  $("#cfg-interject-rate").value = ij.rate_every_messages;
  $("#cfg-interject-fullctx").checked = !!ij.full_context;
  $("#cfg-interject-prob").value = Math.round(ij.base_probability * 100);
  $("#cfg-interject-maxtok").value = ij.interject_max_tokens;
  $("#cfg-interject-window").value = ij.activity_window_minutes;
  $("#cfg-soft-at").checked = ij.soft_at_reply;
  $("#cfg-names").value = ij.names;
  $("#cfg-hooks").value = ij.hooks;
  $("#cfg-memory").checked = mem.enabled;
  $("#cfg-mem-placement").value = mem.placement;
  $("#cfg-mem-auto").checked = mem.auto_placement;
  $("#cfg-mem-cooldown").value = (mem.auto_cooldown_minutes / 60);
  $("#cfg-recent-tokens").value = c.recent_max_tokens;
  $("#cfg-recent-keep").value = c.recent_keep_msgs;
  $("#cfg-mem-max").value = mem.max_entries;
  $("#cfg-mem-chars").value = mem.max_entry_chars;
  $("#cfg-mem-tokens").value = mem.max_tokens;
  $("#cfg-trail").checked = c.trail.enabled;
  $("#cfg-trail-mode").value = c.trail.inject_mode || "window";
  $("#cfg-trail-window").value = c.trail.window_minutes;
  $("#cfg-trail-max").value = c.trail.max_entries;
  $("#cfg-trail-tokens").value = c.trail.max_tokens;
  syncModeFields();
  syncTrailModeFields();
  syncInterjectFields();
}

function collectForm() {
  const n = cfg.napcat, c = cfg.chat, ij = c.interject, mem = c.memory;
  n.mode = $("#cfg-mode").value;
  n.ws_url = $("#cfg-ws-url").value.trim();
  n.reverse_port = parseInt($("#cfg-reverse-port").value) || 3005;
  n.access_token = $("#cfg-token").value.trim();
  n.self_id = $("#cfg-self").value.trim();
  n.group_trigger = $("#cfg-trigger").value;
  n.keyword = $("#cfg-keyword").value;
  n.reply_quoted = $("#cfg-reply-quoted").checked;
  n.reply_pending = $("#cfg-reply-pending").checked;
  n.pending_delay_secs = parseInt($("#cfg-pending-delay").value) || 15;
  n.pending_text = $("#cfg-pending-text").value;
  n.max_msg_len = parseInt($("#cfg-max-len").value) || 1800;
  n.segment_delay_ms = parseInt($("#cfg-seg-delay").value) || 300;
  c.enable_group = $("#cfg-enable-group").checked;
  c.enable_private = $("#cfg-enable-private").checked;
  c.decider = $("#cfg-decider").checked;
  c.context_tokens = parseInt($("#cfg-context").value) || 65536;
  c.reserve_tokens = parseInt($("#cfg-reserve").value) || 1024;
  c.history_target_tokens = parseInt($("#cfg-history-target").value) || 0;
  c.summarize = $("#cfg-summarize").checked;
  c.summarize_tokens = parseInt($("#cfg-summarize-tokens").value) || 600;
  c.clean_after_hours = parseInt($("#cfg-clean-hours").value) || 0;
  c.estimate_ratio = parseFloat($("#cfg-est-ratio").value) || 1.15;
  c.ignore_prefix_enabled = $("#cfg-ignore-star").checked;
  c.ignore_prefix = $("#cfg-ignore-prefix").value.trim() || "*";
  cfg.cost.wallet_balance = parseFloat($("#cfg-wallet").value) || 0;
  cfg.ui_refresh_ms = parseInt($("#cfg-ui-refresh").value) || 500;
  ij.enabled = $("#cfg-interject").checked;
  ij.mode = $("#cfg-interject-mode").value;
  ij.cooldown_messages = parseInt($("#cfg-interject-cooldown").value) || 25;
  ij.rate_every_messages = parseInt($("#cfg-interject-rate").value) || 5;
  ij.full_context = $("#cfg-interject-fullctx").checked;
  ij.base_probability = (parseFloat($("#cfg-interject-prob").value) || 5) / 100;
  ij.interject_max_tokens = parseInt($("#cfg-interject-maxtok").value) || 120;
  ij.activity_window_minutes = parseInt($("#cfg-interject-window").value) || 2;
  ij.soft_at_reply = $("#cfg-soft-at").checked;
  ij.names = $("#cfg-names").value.trim();
  ij.hooks = $("#cfg-hooks").value.trim();
  mem.enabled = $("#cfg-memory").checked;
  mem.placement = $("#cfg-mem-placement").value;
  mem.auto_placement = $("#cfg-mem-auto").checked;
  mem.auto_cooldown_minutes = Math.round((parseFloat($("#cfg-mem-cooldown").value) || 2) * 60);
  c.recent_max_tokens = parseInt($("#cfg-recent-tokens").value) || 3000;
  c.recent_keep_msgs = parseInt($("#cfg-recent-keep").value) || 10;
  mem.max_entries = parseInt($("#cfg-mem-max").value) || 30;
  mem.max_entry_chars = parseInt($("#cfg-mem-chars").value) || 200;
  mem.max_tokens = parseInt($("#cfg-mem-tokens").value) || 1200;
  c.trail.enabled = $("#cfg-trail").checked;
  c.trail.inject_mode = $("#cfg-trail-mode").value;
  c.trail.window_minutes = parseInt($("#cfg-trail-window").value) || 5;
  c.trail.max_entries = parseInt($("#cfg-trail-max").value) || 10;
  c.trail.max_tokens = parseInt($("#cfg-trail-tokens").value) || 800;
  cfg.active_model = $("#cfg-active-model").value;
  cfg.active_prompt = $("#cfg-active-prompt").value;
  return cfg;
}

function syncModeFields() {
  const reverse = $("#cfg-mode").value === "reverse";
  $("#lbl-ws-url").style.display = reverse ? "none" : "";
  $("#lbl-reverse-port").style.display = reverse ? "" : "none";
}
$("#cfg-mode").addEventListener("change", syncModeFields);

// 轨迹注入:只有「窗口注入」激活时间窗口设置(条数与 token 上限作为安全上限仍显示)
function syncTrailModeFields() {
  const windowMode = $("#cfg-trail-mode").value === "window";
  $("#cfg-trail-window").disabled = !windowMode;
  $("#cfg-trail-window").closest("label").classList.toggle("disabled-field", !windowMode);
}
$("#cfg-trail-mode").addEventListener("change", syncTrailModeFields);

// 插话模式:固定频率模式显示「每 N 条」设置
function syncInterjectFields() {
  const fixedRate = $("#cfg-interject-mode").value === "fixed_rate";
  $("#lbl-interject-rate").style.display = fixedRate ? "" : "none";
}
$("#cfg-interject-mode").addEventListener("change", syncInterjectFields);

// ---------- 保存 ----------
$("#btn-save").addEventListener("click", async () => {
  try {
    collectForm();
    await invoke("save_config", { cfg });
    setResult(null, "✅ 配置已保存" + (running ? "并已应用" : "(启动后生效)"));
    addLog("info", "配置已保存");
    renderModels();
    renderPrompts();
    renderOverview();
    refreshCost();
    startEventPoller(); // 刷新间隔可能已变更,按新值重启轮询
  } catch (e) {
    setResult(String(e), "err");
    addLog("error", "保存配置失败: " + e);
  }
});

function setResult(text, cls) {
  const el = $("#test-napcat-result");
  el.textContent = text || "";
  el.className = "result " + (cls || "");
}

// ---------- 启动/停止 ----------
$("#btn-toggle").addEventListener("click", async () => {
  const btn = $("#btn-toggle");
  btn.disabled = true;
  try {
    if (running) {
      await invoke("stop_bot");
      addLog("info", "机器人已停止");
    } else {
      collectForm();
      // 未保存的修改先保存
      await invoke("save_config", { cfg });
      await invoke("start_bot");
      addLog("info", "机器人已启动");
    }
    running = !running;
    paused = false;
    updatePausedUi();
    btn.textContent = running ? "停止" : "启动";
    if (running) {
      // 启动后的过渡状态;稍后主动拉一次真实状态(不依赖事件链路)
      $("#st-dot").className = "dot yellow";
      $("#st-text").textContent = "启动中…";
      setTimeout(async () => {
        try {
          const sv = await invoke("get_status_view");
          if (running) updateStatusView({ connected: sv.connected, mode: sv.mode, endpoint: sv.endpoint, self_id: sv.self_id, last_error: sv.last_error });
        } catch (e) { /* 忽略 */ }
      }, 800);
    } else {
      $("#st-dot").className = "dot gray";
      $("#st-text").textContent = "未启动";
    }
    $("#ov-running").textContent = running ? "运行中" : "未启动";
    // 启动/停止后刷新会话列表(不依赖事件链路)
    refreshSessions();
    renderMemories();
  } catch (e) {
    addLog("error", "操作失败: " + e);
    alert("操作失败: " + e);
  }
  btn.disabled = false;
});

// ---------- 连接测试 ----------
$("#btn-test-napcat").addEventListener("click", async () => {
  const btn = $("#btn-test-napcat");
  btn.disabled = true;
  setResult("测试中…", "");
  try {
    const r = await invoke("test_napcat", {
      mode: $("#cfg-mode").value,
      wsUrl: $("#cfg-ws-url").value.trim(),
      reversePort: parseInt($("#cfg-reverse-port").value) || 3005,
      accessToken: $("#cfg-token").value.trim(),
    });
    setResult(r, "ok");
    addLog("info", r);
  } catch (e) {
    setResult(String(e), "err");
    addLog("error", "连接测试失败: " + e);
  }
  btn.disabled = false;
});

// ---------- 模型管理 ----------
function renderModels() {
  const sel = $("#cfg-active-model");
  sel.innerHTML = "";
  for (const m of cfg.models) {
    const opt = document.createElement("option");
    opt.value = m.name; opt.textContent = m.name;
    if (m.name === cfg.active_model) opt.selected = true;
    sel.appendChild(opt);
  }
  $("#ov-model").textContent = cfg.active_model;
  const active = cfg.models.find((m) => m.name === cfg.active_model) || cfg.models[0];
  const thinkingMap = { auto: "思考·模型默认", enabled: "思考·开启", disabled: "非思考" };
  $("#ov-kind").textContent = active ? (thinkingMap[active.thinking] || "对话") : "-";
  $("#ov-prompt").textContent = cfg.active_prompt;
  $("#ov-budget").textContent = cfg.chat.context_tokens + " tokens";

  const list = $("#model-list");
  list.innerHTML = "";
  cfg.models.forEach((m, i) => {
    const card = document.createElement("div");
    card.className = "item-card";
    card.innerHTML = `
      <div class="item-head">
        <b>${escapeHtml(m.name)}</b>
        <span class="badge">${m.kind === "reasoner" ? "推理" : "对话"}</span>
        <span class="spacer"></span>
        <button class="btn small test-model" data-i="${i}">测试</button>
        <button class="btn small danger del-model" data-i="${i}">删除</button>
      </div>
      <div class="item-grid">
        <label>名称<input data-f="name" value="${escapeHtml(m.name)}" /></label>
        <label>API 地址<input data-f="base_url" value="${escapeHtml(m.base_url)}" /></label>
        <label>API Key<input data-f="api_key" type="password" value="${escapeHtml(m.api_key)}" /></label>
        <label>模型名<input data-f="model" value="${escapeHtml(m.model)}" /></label>
        <label>思考模式<select data-f="thinking"><option value="auto" ${m.thinking === "auto" ? "selected" : ""}>自动(模型默认)</option><option value="enabled" ${m.thinking === "enabled" ? "selected" : ""}>开启</option><option value="disabled" ${m.thinking === "disabled" ? "selected" : ""}>关闭</option></select></label>
        <label>推理强度(思考时生效)<select data-f="reasoning_effort"><option value="low" ${m.reasoning_effort === "low" ? "selected" : ""}>低</option><option value="high" ${m.reasoning_effort === "high" ? "selected" : ""}>高(默认)</option><option value="max" ${m.reasoning_effort === "max" ? "selected" : ""}>最高</option></select></label>
        <label>温度<input data-f="temperature" type="number" step="0.1" min="0" max="2" value="${m.temperature}" /></label>
        <label>max_tokens<input data-f="max_tokens" type="number" min="16" value="${m.max_tokens}" /></label>
        <label>超时(秒)<input data-f="timeout_secs" type="number" min="10" value="${m.timeout_secs}" /></label>
        <label>输入价·命中缓存(元/1M)<input data-f="price_cache_hit" type="number" step="0.001" min="0" value="${m.price_cache_hit}" /></label>
        <label>输入价·未命中(元/1M)<input data-f="price_input" type="number" step="0.01" min="0" value="${m.price_input}" /></label>
        <label>输出价(元/1M)<input data-f="price_output" type="number" step="0.01" min="0" value="${m.price_output}" /></label>
      </div>
      <div class="result test-result" data-i="${i}"></div>`;
    list.appendChild(card);
  });

  list.querySelectorAll("[data-f]").forEach((el) => {
    const i = parseInt(el.closest(".item-card").querySelector(".test-model").dataset.i);
    const f = el.dataset.f;
    el.addEventListener("input", () => {
      const v = el.type === "number" ? parseFloat(el.value) || 0 : el.value;
      cfg.models[i][f] = v;
    });
  });
  list.querySelectorAll(".del-model").forEach((btn) => {
    btn.addEventListener("click", () => {
      const i = parseInt(btn.dataset.i);
      if (cfg.models.length <= 1) { alert("至少保留一个模型"); return; }
      const removed = cfg.models[i].name;
      cfg.models.splice(i, 1);
      if (cfg.active_model === removed) cfg.active_model = cfg.models[0]?.name || "";
      renderModels();
    });
  });
  list.querySelectorAll(".test-model").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const i = parseInt(btn.dataset.i);
      const m = { ...cfg.models[i] };
      const res = btn.closest(".item-card").querySelector(".test-result");
      btn.disabled = true;
      res.textContent = "测试中…";
      res.className = "result test-result";
      try {
        const r = await invoke("test_llm", { m });
        res.textContent = r;
        res.className = "result test-result ok";
        addLog("info", r);
      } catch (e) {
        res.textContent = String(e);
        res.className = "result test-result err";
        addLog("error", `模型 ${m.name} 测试失败: ${e}`);
      }
      btn.disabled = false;
    });
  });
}

$("#btn-add-model").addEventListener("click", () => {
  cfg.models.push({
    name: "new-model-" + (cfg.models.length + 1),
    base_url: "https://api.deepseek.com",
    api_key: "",
    model: "deepseek-v4-flash",
    kind: "chat",
    thinking: "auto",
    reasoning_effort: "high",
    temperature: 1.0,
    max_tokens: 8192,
    timeout_secs: 120,
    price_input: 1.0,
    price_cache_hit: 0.02,
    price_output: 2.0,
  });
  renderModels();
});

// ---------- 人设管理 ----------
function renderPrompts() {
  const sel = $("#cfg-active-prompt");
  sel.innerHTML = "";
  for (const p of cfg.prompts) {
    const opt = document.createElement("option");
    opt.value = p.id; opt.textContent = p.name + " (" + p.id + ")";
    if (p.id === cfg.active_prompt) opt.selected = true;
    sel.appendChild(opt);
  }
  const list = $("#prompt-list");
  list.innerHTML = "";
  cfg.prompts.forEach((p, i) => {
    const card = document.createElement("div");
    card.className = "item-card";
    card.innerHTML = `
      <div class="item-head">
        <b>${escapeHtml(p.name)}</b>
        <span class="badge">${escapeHtml(p.id)}</span>
        <span class="spacer"></span>
        <button class="btn small danger del-prompt" data-i="${i}">删除</button>
      </div>
      <div class="item-grid">
        <label>ID<input data-f="id" value="${escapeHtml(p.id)}" /></label>
        <label>名称<input data-f="name" value="${escapeHtml(p.name)}" /></label>
        <label style="grid-column: 1 / -1">System Prompt<textarea data-f="prompt">${escapeHtml(p.prompt)}</textarea></label>
      </div>`;
    list.appendChild(card);
  });
  list.querySelectorAll("[data-f]").forEach((el) => {
    const i = parseInt(el.closest(".item-card").querySelector(".del-prompt").dataset.i);
    const f = el.dataset.f;
    el.addEventListener("input", () => { cfg.prompts[i][f] = el.value; });
  });
  list.querySelectorAll(".del-prompt").forEach((btn) => {
    btn.addEventListener("click", () => {
      const i = parseInt(btn.dataset.i);
      if (cfg.prompts.length <= 1) { alert("至少保留一个人设"); return; }
      const removed = cfg.prompts[i].id;
      cfg.prompts.splice(i, 1);
      if (cfg.active_prompt === removed) cfg.active_prompt = cfg.prompts[0].id;
      renderPrompts();
    });
  });
}

$("#btn-add-prompt").addEventListener("click", () => {
  cfg.prompts.push({
    id: "preset-" + (cfg.prompts.length + 1),
    name: "新的人设",
    prompt: "你是一个乐于助人的 AI 助手。",
  });
  renderPrompts();
});

// ---------- 5. 状态胶囊 ----------
const STATUS_META = {
  idle:      { label: "空闲",   cls: "st-idle" },
  replying:  { label: "回复中", cls: "st-replying" },
  thinking:  { label: "思考中", cls: "st-thinking" },
  deciding:  { label: "决策中", cls: "st-deciding" },
  executing: { label: "执行中", cls: "st-executing" },
  approval:  { label: "审批中", cls: "st-approval" },
};

function statusCapsule(status) {
  const m = STATUS_META[status] || STATUS_META.idle;
  const span = document.createElement("span");
  span.className = "capsule " + m.cls;
  span.textContent = m.label;
  return span;
}

function updateSessionStatusCell(key, status) {
  const row = document.querySelector(`#session-table tbody tr[data-key="${CSS.escape(key)}"]`);
  if (!row) return;
  const cell = row.querySelector(".status-cell");
  if (!cell) return;
  cell.innerHTML = "";
  cell.appendChild(statusCapsule(status));
}

// ---------- 5b. 会话列表 ----------
async function refreshSessions() {
  const tbody = $("#session-table tbody");
  tbody.innerHTML = "";
  let list = [];
  try { list = await invoke("get_sessions"); } catch (e) { /* 未运行 */ }
  for (const s of list) {
    const tr = document.createElement("tr");
    tr.dataset.key = s.key;
    const kind = s.key.startsWith("g") ? "群" : "私聊";
    tr.innerHTML = `
      <td class="status-cell"></td>
      <td>${kind} ${escapeHtml(s.key.slice(1))}</td>
      <td>${s.count}</td>
      <td>${s.tokens}</td>
      <td>${s.has_summary ? "✓" : "-"}</td>
      <td class="row-actions">
        <button class="btn small danger clear-session">清空</button>
        <span class="menu-wrap">
          <button class="btn small row-more" title="更多操作">⋯</button>
          <span class="row-menu hidden">
            <button class="row-menu-item clear-trace">清空历史(轨迹)</button>
          </span>
        </span>
      </td>`;
    tr.querySelector(".status-cell").appendChild(statusCapsule(s.status || "idle"));
    tr.querySelector(".status-cell").dataset.status = s.status || "idle";
    // 整行可点开详情页;按钮单独处理(不冒泡)
    tr.addEventListener("click", () => openSessionDetail(s.key));
    tr.querySelector(".clear-session").addEventListener("click", async (e) => {
      e.stopPropagation();
      if (!confirm(`确认清空会话 ${s.key} 的上下文?`)) return;
      try {
        await invoke("clear_session", { key: s.key });
        refreshSessions();
      } catch (err) { addLog("error", "清空失败: " + err); }
    });
    // ⋯ 下拉菜单
    const moreBtn = tr.querySelector(".row-more");
    const menu = tr.querySelector(".row-menu");
    moreBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      $$(".row-menu").forEach((m) => { if (m !== menu) m.classList.add("hidden"); });
      menu.classList.toggle("hidden");
    });
    tr.querySelector(".clear-trace").addEventListener("click", async (e) => {
      e.stopPropagation();
      menu.classList.add("hidden");
      if (!confirm(`确认清空会话 ${s.key} 的历史轨迹(详情页时间线)?上下文不受影响。`)) return;
      try {
        await invoke("clear_trace", { key: s.key });
        addLog("info", `已清空 ${s.key} 的历史轨迹`);
        if (detailState.key === s.key) openSessionDetail(s.key);
      } catch (err) { addLog("error", "清空轨迹失败: " + err); }
    });
    tbody.appendChild(tr);
  }
}

// 点击任意位置关闭 ⋯ 菜单
document.addEventListener("click", () => {
  $$(".row-menu").forEach((m) => m.classList.add("hidden"));
});

// ---------- 5c. 记忆管理 ----------
async function renderMemories() {
  const box = $("#memory-list");
  box.innerHTML = "";
  let list = [];
  try { list = await invoke("get_all_memories"); } catch (e) { box.textContent = "加载失败: " + e; return; }
  if (!list.length) {
    box.innerHTML = '<div class="tips">暂无记忆。对话中模型会自动写入,或通过 QQ 命令 /remember <内容> 添加。</div>';
    return;
  }
  for (const s of list) {
    const card = document.createElement("div");
    card.className = "item-card";
    const kind = s.key.startsWith("g") ? "群" : "私聊";
    let rows = "";
    for (const e of s.entries) {
      rows += `<div class="kv">
        <span>${e.index}. [${e.source === "model" ? "自动" : "用户"} ${e.date}] ${escapeHtml(e.text)}</span>
        <button class="btn small danger del-mem" data-key="${escapeHtml(s.key)}" data-idx="${e.index}">删</button>
      </div>`;
    }
    card.innerHTML = `
      <div class="item-head"><b>${kind} ${escapeHtml(s.key.slice(1))}</b><span class="badge">${s.entries.length} 条</span></div>
      <div style="display:flex;flex-direction:column;gap:6px">${rows}</div>
      <div class="add-mem-row" style="display:flex;gap:8px;margin-top:10px">
        <input type="text" class="mem-text" placeholder="添加记忆…" style="flex:1" />
        <button class="btn small add-mem" data-key="${escapeHtml(s.key)}">添加</button>
      </div>`;
    box.appendChild(card);
  }
  box.querySelectorAll(".del-mem").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        await invoke("delete_memory", { key: btn.dataset.key, index: parseInt(btn.dataset.idx) });
        renderMemories();
      } catch (e) { alert("删除失败: " + e); }
    });
  });
  box.querySelectorAll(".add-mem").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const input = btn.closest(".add-mem-row").querySelector(".mem-text");
      const text = input.value.trim();
      if (!text) return;
      try {
        await invoke("add_memory", { key: btn.dataset.key, text });
        renderMemories();
      } catch (e) { alert("添加失败: " + e); }
    });
  });
}

$("#btn-refresh-sessions").addEventListener("click", refreshSessions);
$("#btn-refresh-mem").addEventListener("click", renderMemories);

// ---------- 6. 今日开销面板 ----------
const CATEGORY_LABELS = {
  dialogue: "对话",
  decide: "决策器",
  summarize: "摘要",
  interject: "插话",
};

let costRefreshTimer = null;
function scheduleCostRefresh() {
  if (costRefreshTimer) return;
  costRefreshTimer = setTimeout(() => {
    costRefreshTimer = null;
    refreshCost();
  }, 1200);
}

async function refreshCost() {
  let d;
  try { d = await invoke("get_cost_overview"); } catch (e) { return; }
  const t = d.today;
  $("#cost-tokens").textContent = (t.prompt + t.completion).toLocaleString() + " tokens";
  $("#cost-hit-miss").textContent =
    `${(t.cache_hit || 0).toLocaleString()} / ${(t.cache_miss || 0).toLocaleString()}`;
  $("#cost-yuan").textContent = "¥ " + t.cost.toFixed(4);
  const walletEl = $("#cost-wallet");
  walletEl.textContent = `¥ ${d.remaining.toFixed(4)}(余额 ¥${d.wallet_balance.toFixed(2)})`;
  walletEl.classList.toggle("neg", d.remaining < 0);

  // 条形图:按类别展示 命中 / 未命中 / 输出 的堆叠条
  const box = $("#cost-bars");
  box.innerHTML = "";
  const cats = d.by_category || [];
  const maxTotal = Math.max(1, ...cats.map(([k, v]) => v.cache_hit + v.cache_miss + v.completion));
  for (const [key, v] of cats) {
    const label = CATEGORY_LABELS[key] || key;
    const row = document.createElement("div");
    row.className = "bar-row";
    const total = v.cache_hit + v.cache_miss + v.completion;
    row.innerHTML = `
      <span class="bar-label">${label}<small>${v.calls} 次 · ¥${v.cost.toFixed(4)}</small></span>
      <div class="bar-track">
        <div class="bar-seg hit" style="width:${(v.cache_hit / maxTotal) * 100}%" title="命中 ${v.cache_hit.toLocaleString()}"></div>
        <div class="bar-seg miss" style="width:${(v.cache_miss / maxTotal) * 100}%" title="未命中 ${v.cache_miss.toLocaleString()}"></div>
        <div class="bar-seg out" style="width:${(v.completion / maxTotal) * 100}%" title="输出 ${v.completion.toLocaleString()}"></div>
      </div>
      <span class="bar-nums">${total.toLocaleString()}t</span>`;
    box.appendChild(row);
  }
  if (!cats.length) {
    box.innerHTML = '<div class="muted" style="font-size:12px;padding:6px 0">今天还没有模型调用。</div>';
  }
}

$("#btn-refresh-cost").addEventListener("click", refreshCost);

// 查询 DeepSeek 账户余额(当前激活模型 key)
$("#btn-query-balance").addEventListener("click", async () => {
  const btn = $("#btn-query-balance");
  const res = $("#balance-result");
  btn.disabled = true;
  res.textContent = "查询中…";
  res.className = "result";
  try {
    const d = await invoke("query_balance");
    const b = (d.balances || [])[0];
    if (!b) {
      res.textContent = "未返回余额信息";
      res.className = "result err";
    } else {
      const state = d.is_available ? "" : " · 当前不可用";
      res.textContent =
        `✓ ${d.model}: 总余额 ¥${b.total.toFixed(2)}` +
        `(充值 ¥${b.topped_up.toFixed(2)} + 赠送 ¥${b.granted.toFixed(2)})${state}`;
      res.className = "result ok";
      // 自动填入钱包余额(点「保存配置」落盘)
      cfg.cost.wallet_balance = b.total;
      $("#cfg-wallet").value = b.total.toFixed(2);
      refreshCost();
      addLog("info", `余额查询: ${d.model} 总余额 ¥${b.total.toFixed(2)}`);
    }
  } catch (e) {
    res.textContent = String(e);
    res.className = "result err";
    addLog("error", "余额查询失败: " + e);
  }
  btn.disabled = false;
});

// ---------- 7. 会话详情页 ----------
const detailState = {
  key: null,
  turnBlocks: new Map(), // turn -> {el, thinkCard, outCard}
  renderedCount: 0,      // 已渲染的持久化事件数(轮询只追加新事件,避免重复)
};

function fmtTime(ts) {
  if (!ts) return "";
  const d = new Date(ts * 1000);
  const pad = (x) => String(x).padStart(2, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function tlCard(kind, title, extraActions = "") {
  const card = document.createElement("div");
  card.className = "tl-card tl-" + kind;
  card.innerHTML = `
    <div class="tl-head">
      <span class="tl-title">${title}</span>
      <span class="spacer"></span>
      ${extraActions}
    </div>`;
  return card;
}

function tlBody(card, text) {
  const body = document.createElement("div");
  body.className = "tl-body";
  body.textContent = text;
  card.appendChild(body);
  return body;
}

function turnBlock(turn) {
  let block = detailState.turnBlocks.get(turn);
  if (!block) {
    const el = document.createElement("div");
    el.className = "tl-turn";
    el.dataset.turn = turn;
    $("#detail-timeline").appendChild(el);
    block = { el, thinkCard: null, outCard: null };
    detailState.turnBlocks.set(turn, block);
  }
  return block;
}

function appendTurnCard(turn, card) {
  const block = turnBlock(turn);
  block.el.appendChild(card);
  return block;
}

function editButtons(id) {
  if (!id) return "";
  return `<button class="btn small tl-edit" data-id="${escapeHtml(id)}">编辑</button>
          <button class="btn small danger tl-del" data-id="${escapeHtml(id)}">删除</button>`;
}

function renderDetailEntry(ev) {
  const timeline = $("#detail-timeline");
  switch (ev.type) {
    case "msg_in": {
      const triggerLabel = { at: "at 触发", reply: "引用回复", keyword: "关键词", private: "私聊", ignored: "★ 忽略", decided_no: "决策器拒绝", untriggered: "未触发", paused: "⏸ 仅接收(已暂停)" }[ev.trigger] || ev.trigger;
      const card = tlCard(ev.ignored ? "msg-ignored" : "msg-in",
        `👤 用户消息 · ${triggerLabel} · ${fmtTime(ev.ts)}`,
        editButtons(ev.id));
      tlBody(card, ev.text);
      appendTurnCard(ev.turn, card);
      break;
    }
    case "cmd": {
      const card = tlCard("cmd", `⚙ 用户命令 · ${fmtTime(ev.ts)}`);
      tlBody(card, ev.text);
      const rep = document.createElement("div");
      rep.className = "tl-reply";
      rep.textContent = ev.reply;
      card.appendChild(rep);
      appendTurnCard(ev.turn, card);
      break;
    }
    case "decide": {
      const card = tlCard(ev.verdict ? "decide-yes" : "decide-no",
        `⚖ 决策器:${ev.verdict ? "需要回复" : "无需回复"} · ${ev.model} · ${ev.ms}ms · ${fmtTime(ev.ts)}`);
      tlBody(card, ev.text);
      card.querySelector(".tl-body").classList.add("collapsible-body");
      card.querySelector(".tl-title").style.cursor = "pointer";
      card.querySelector(".tl-title").addEventListener("click", () => {
        card.querySelector(".tl-body").classList.toggle("folded");
      });
      appendTurnCard(ev.turn, card);
      break;
    }
    case "think": {
      const card = tlCard("think",
        `💭 思考过程 · ${(ev.tokens || 0).toLocaleString()} tokens · ${fmtTime(ev.ts)}`);
      const body = tlBody(card, ev.text);
      body.classList.add("collapsible-body");
      card.querySelector(".tl-title").style.cursor = "pointer";
      card.querySelector(".tl-title").addEventListener("click", () => body.classList.toggle("folded"));
      appendTurnCard(ev.turn, card);
      break;
    }
    case "msg_out": {
      const u = ev.usage || {};
      const ratio = u.cache_hit + u.cache_miss > 0 ? Math.round((u.cache_hit / (u.cache_hit + u.cache_miss)) * 100) : 0;
      const card = tlCard("msg-out",
        `🤖 AI 回复 · ${escapeHtml(ev.model)} · ${fmtTime(ev.ts)}`,
        editButtons(ev.id));
      tlBody(card, ev.text);
      const foot = document.createElement("div");
      foot.className = "tl-foot";
      foot.textContent = `prompt ${(u.prompt_tokens || 0).toLocaleString()}t · 命中 ${(u.cache_hit || 0).toLocaleString()}t(${ratio}%) · 输出 ${(u.completion_tokens || 0).toLocaleString()}t${u.reasoning_tokens ? ` · 思考 ${u.reasoning_tokens.toLocaleString()}t` : ""}`;
      card.appendChild(foot);
      appendTurnCard(ev.turn, card);
      break;
    }
    case "lite_out": {
      const card = tlCard("lite-out", `🤖 主动插话(AI) · ${escapeHtml(ev.model)} · ${fmtTime(ev.ts)}`);
      tlBody(card, ev.text);
      appendTurnCard(ev.turn, card);
      break;
    }
    case "fold": {
      const card = tlCard("fold", `🧾 摘要折叠 · ${ev.folded} 条旧消息 · 摘要 ${(ev.summary_tokens || 0).toLocaleString()}t · ${fmtTime(ev.ts)}`);
      appendTurnCard(ev.turn, card);
      break;
    }
    case "error": {
      const card = tlCard("error", `✗ ${escapeHtml(ev.text)} · ${fmtTime(ev.ts)}`);
      appendTurnCard(ev.turn, card);
      break;
    }
  }
  timeline.scrollTop = timeline.scrollHeight;
}

async function openSessionDetail(key) {
  detailState.key = key;
  detailState.turnBlocks.clear();
  $("#session-detail").classList.remove("hidden");
  const title = key.startsWith("g") ? `群 ${key.slice(1)}` : `私聊 ${key.slice(1)}`;
  $("#detail-title").textContent = title;
  $("#detail-timeline").innerHTML = "";
  let d;
  try {
    d = await invoke("get_session_detail", { key });
  } catch (e) {
    addLog("error", "加载会话详情失败: " + e);
    return;
  }
  setDetailStatus(d.status);
  $("#detail-meta").textContent = `${d.count} 条消息 · 约 ${(d.tokens || 0).toLocaleString()} tokens${d.has_summary ? " · 含摘要" : ""}`;
  const sumBox = $("#detail-summary");
  if (d.has_summary && d.summary) {
    sumBox.classList.remove("hidden");
    sumBox.textContent = "📑 摘要:" + d.summary;
  } else {
    sumBox.classList.add("hidden");
  }
  for (const ev of d.events || []) {
    renderDetailEntry(ev);
  }
  detailState.renderedCount = (d.events || []).length;
  // 打开时若该会话正在回复:直接渲染直播缓冲(事件与轮询双保险)
  if (d.live) syncLiveTurn(d.live);
  // 为编辑/删除按钮绑定(渲染后统一绑定)
  bindDetailActions();
}

function setDetailStatus(status) {
  // 直接替换自身类名与文本,不再向 #detail-status 内嵌套胶囊(修复双层胶囊)
  const m = STATUS_META[status] || STATUS_META.idle;
  const el = $("#detail-status");
  el.className = "capsule " + m.cls;
  el.textContent = m.label;
}

function bindDetailActions() {
  $$("#detail-timeline .tl-edit").forEach((btn) => {
    if (btn.dataset.bound) return; // 避免直播事件反复追加监听
    btn.dataset.bound = "1";
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const card = btn.closest(".tl-card");
      const body = card.querySelector(".tl-body");
      if (!body || body.querySelector("textarea")) return;
      const original = body.textContent;
      const ta = document.createElement("textarea");
      ta.value = original;
      ta.className = "tl-editarea";
      const save = document.createElement("button");
      save.className = "btn small primary";
      save.textContent = "保存";
      const cancel = document.createElement("button");
      cancel.className = "btn small";
      cancel.textContent = "取消";
      const wrap = document.createElement("div");
      wrap.className = "tl-edit-wrap";
      wrap.appendChild(ta);
      wrap.appendChild(save);
      wrap.appendChild(cancel);
      body.replaceWith(wrap);
      save.addEventListener("click", async () => {
        try {
          await invoke("update_history_msg", { key: detailState.key, id: btn.dataset.id, text: ta.value });
          addLog("info", "聊天记录已改写");
          openSessionDetail(detailState.key);
        } catch (err) { alert("改写失败: " + err); }
      });
      cancel.addEventListener("click", () => openSessionDetail(detailState.key));
    });
  });
  $$("#detail-timeline .tl-del").forEach((btn) => {
    if (btn.dataset.bound) return;
    btn.dataset.bound = "1";
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      if (!confirm("删除这条聊天记录?(同时从模型上下文中移除)")) return;
      try {
        await invoke("delete_history_msg", { key: detailState.key, id: btn.dataset.id });
        addLog("info", "聊天记录已删除");
        openSessionDetail(detailState.key);
      } catch (err) { alert("删除失败: " + err); }
    });
  });
}

$("#btn-detail-back").addEventListener("click", () => {
  $("#session-detail").classList.add("hidden");
  detailState.key = null;
  detailState.turnBlocks.clear();
});

$("#btn-detail-stop").addEventListener("click", async () => {
  try {
    const next = !paused;
    await invoke("set_paused", { paused: next });
    paused = next;
    updatePausedUi();
    addLog("info", next ? "已停止所有回复/决策/思考,仅接收消息" : "已恢复回复");
  } catch (e) { alert("操作失败: " + e); }
});

// 直播:完整轨迹事件 → 触发一次即时详情刷新。
// 渲染统一走轮询路径(单一路径),避免事件+轮询双路径重复渲染卡片。
let detailPollTimer = null;
function scheduleDetailPoll() {
  if (!detailState.key) return;
  if (detailPollTimer) return;
  detailPollTimer = setTimeout(() => {
    detailPollTimer = null;
    pollDetail();
  }, 80);
}

function handleTraceEvent(ev) {
  if (!detailState.key || ev.key !== detailState.key) return;
  scheduleDetailPoll();
}

// 收尾直播输出卡片为最终全量文本
function finalizeOutCard(block, entry) {
  const { card, body } = block.outCard;
  block.outCard = null;
  const u = entry.usage || {};
  const ratio = u.cache_hit + u.cache_miss > 0 ? Math.round((u.cache_hit / (u.cache_hit + u.cache_miss)) * 100) : 0;
  card.querySelector(".tl-title").textContent =
    `🤖 AI 回复 · ${escapeHtml(entry.model)} · ${fmtTime(entry.ts)}`;
  body.textContent = entry.text;
  const foot = document.createElement("div");
  foot.className = "tl-foot";
  foot.textContent = `prompt ${(u.prompt_tokens || 0).toLocaleString()}t · 命中 ${(u.cache_hit || 0).toLocaleString()}t(${ratio}%) · 输出 ${(u.completion_tokens || 0).toLocaleString()}t${u.reasoning_tokens ? ` · 思考 ${u.reasoning_tokens.toLocaleString()}t` : ""}`;
  card.appendChild(foot);
}

// 收尾直播思考卡片为最终全量文本
function finalizeThinkCard(block, entry) {
  const { card, body } = block.thinkCard;
  block.thinkCard = null;
  card.querySelector(".tl-title").textContent =
    `💭 思考过程 · ${(entry.tokens || 0).toLocaleString()} tokens · ${fmtTime(entry.ts)}`;
  body.textContent = entry.text;
}

// 追加一条持久化事件:think/msg_out 若存在直播卡片则原地收尾,否则渲染新卡片
function appendDetailEntry(entry) {
  const block = detailState.turnBlocks.get(entry.turn);
  if (entry.type === "msg_out" && block && block.outCard) {
    finalizeOutCard(block, entry);
    return;
  }
  if (entry.type === "think" && block && block.thinkCard) {
    finalizeThinkCard(block, entry);
    return;
  }
  renderDetailEntry(entry);
}

// 详情页轮询:追加新持久化事件 + 同步直播缓冲(渲染唯一入口)
async function pollDetail() {
  if (!detailState.key) return;
  try {
    const d = await invoke("get_session_detail", { key: detailState.key });
    const evs = d.events || [];
    if (evs.length < detailState.renderedCount) {
      // 轨迹被清空:整体重渲染
      $("#detail-timeline").innerHTML = "";
      detailState.turnBlocks.clear();
      detailState.renderedCount = 0;
    }
    let appended = false;
    while (detailState.renderedCount < evs.length) {
      appendDetailEntry(evs[detailState.renderedCount]);
      detailState.renderedCount += 1;
      appended = true;
    }
    if (appended) bindDetailActions();
    syncLiveTurn(d.live);
  } catch (e) { /* 忽略 */ }
}

// 确保某 turn 的直播思考卡片存在,返回 {card, body}
function ensureLiveThinkCard(turn) {
  const block = turnBlock(turn);
  if (!block.thinkCard) {
    const card = tlCard("think", "💭 思考过程(进行中)");
    const body = tlBody(card, "");
    body.classList.add("collapsible-body");
    card.querySelector(".tl-title").style.cursor = "pointer";
    card.querySelector(".tl-title").addEventListener("click", () => body.classList.toggle("folded"));
    block.el.appendChild(card);
    block.thinkCard = { card, body };
  }
  return block.thinkCard;
}

// 确保某 turn 的直播输出卡片存在,返回 {card, body}
function ensureLiveOutCard(turn) {
  const block = turnBlock(turn);
  if (!block.outCard) {
    const card = tlCard("msg-out", "🤖 AI 回复(进行中)");
    const body = tlBody(card, "");
    block.el.appendChild(card);
    block.outCard = { card, body };
  }
  return block.outCard;
}

// 直播:思考/正文增量(事件驱动;QQ 侧并非流式,详情页直播)
function handleTurnDelta(ev) {
  if (!detailState.key || ev.key !== detailState.key) return;
  if (ev.kind === "think") {
    const { body } = ensureLiveThinkCard(ev.turn);
    body.textContent += ev.text;
  } else if (ev.kind === "out") {
    const { body } = ensureLiveOutCard(ev.turn);
    body.textContent += ev.text;
  }
  $("#detail-timeline").scrollTop = $("#detail-timeline").scrollHeight;
}

// 轮询兜底:把后端直播缓冲同步到详情页(事件链路异常时流式显示仍可用)
function syncLiveTurn(live) {
  if (!live || !detailState.key) return;
  turnBlock(live.turn);
  if (live.reasoning) {
    const { body } = ensureLiveThinkCard(live.turn);
    if (body.textContent.length <= live.reasoning.length) {
      body.textContent = live.reasoning;
    }
  }
  if (live.content) {
    const { body } = ensureLiveOutCard(live.turn);
    if (body.textContent.length <= live.content.length) {
      body.textContent = live.content;
    }
  }
  $("#detail-timeline").scrollTop = $("#detail-timeline").scrollHeight;
}

// ---------- 8. 记忆位置切换审批弹窗 ----------
function showApprovalModal(p) {
  const name = { front: "方案二(摘要→记忆→新历史→提问)", back: "方案一(历史→记忆→提问)" };
  const cdMin = (cfg?.chat?.memory?.auto_cooldown_minutes) || 120;
  const cdText = cdMin >= 60 ? (cdMin / 60) + " 小时" : cdMin + " 分钟";
  const body = $("#approval-body");
  body.innerHTML = "";
  const info = document.createElement("div");
  info.className = "approval-info";
  info.innerHTML = `
    <p><b>建议从</b> ${escapeHtml(name[p.from] || p.from)}
       <b>切换到</b> ${escapeHtml(name[p.to] || p.to)}</p>
    <p class="approval-reason">${escapeHtml(p.reason || "")}</p>
    <div class="approval-nums">
      <span>每轮节省 <b>¥${(p.saving_per_round / 1e6).toFixed(6)}</b></span>
      <span>一次性切换成本 <b>¥${(p.switch_cost / 1e6).toFixed(4)}</b></span>
      <span>展望 ${p.horizon} 轮净省 <b>¥${(p.expected_saving / 1e6).toFixed(2)}</b></span>
    </div>
    <p class="muted" style="font-size:12px">切换会立即重启机器人并导致一次大范围缓存未命中;批准或拒绝后 ${escapeHtml(cdText)} 内不再提出新的切换(冷却时长可在会话设置调整)。</p>`;
  body.appendChild(info);
  $("#approval-modal").classList.remove("hidden");
}

$("#btn-approve-yes").addEventListener("click", async () => {
  try {
    const applied = await invoke("approve_placement", { approve: true });
    $("#approval-modal").classList.add("hidden");
    addLog("info", applied ? `已切换到记忆位置策略: ${applied}` : "提案已过期,未切换");
    if (applied) {
      cfg.chat.memory.placement = applied;
      $("#cfg-mem-placement").value = applied;
      // 重启后刷新状态
      setTimeout(async () => {
        try {
          const sv = await invoke("get_status_view");
          running = sv.running;
          $("#btn-toggle").textContent = running ? "停止" : "启动";
          updateStatusView({ connected: sv.connected, mode: sv.mode, endpoint: sv.endpoint, self_id: sv.self_id, last_error: sv.last_error });
        } catch (e) { /* 忽略 */ }
      }, 1200);
    }
  } catch (e) {
    alert("切换失败: " + e);
  }
});

$("#btn-approve-no").addEventListener("click", async () => {
  $("#approval-modal").classList.add("hidden");
  try {
    await invoke("approve_placement", { approve: false });
    const cdMin = (cfg?.chat?.memory?.auto_cooldown_minutes) || 120;
    const cdText = cdMin >= 60 ? (cdMin / 60) + " 小时" : cdMin + " 分钟";
    addLog("info", `已拒绝切换提案,进入 ${cdText} 冷却`);
  } catch (e) { /* 忽略 */ }
});

// ---------- 实时更新兜底(轮询:即使事件链路异常,列表/详情页也会自动刷新) ----------
function patchSessionRow(s) {
  const tbody = $("#session-table tbody");
  const row = tbody.querySelector(`tr[data-key="${CSS.escape(s.key)}"]`);
  if (!row) { refreshSessions(); return; }
  const tds = row.children; // 状态(0) 会话(1) 消息数(2) tokens(3) 摘要(4) 操作(5)
  const want = s.status || "idle";
  if (tds[0].dataset.status !== want) {
    tds[0].dataset.status = want;
    tds[0].innerHTML = "";
    tds[0].appendChild(statusCapsule(want));
  }
  if (tds[2].textContent !== String(s.count)) tds[2].textContent = s.count;
  if (tds[3].textContent !== String(s.tokens)) tds[3].textContent = s.tokens;
  const sum = s.has_summary ? "✓" : "-";
  if (tds[4].textContent !== sum) tds[4].textContent = sum;
}

async function pollLive() {
  if (!running) return;
  // 暂停状态同步(可能来自其他入口)
  try {
    const sv = await invoke("get_status_view");
    if (!!sv.paused !== paused) {
      paused = !!sv.paused;
      updatePausedUi();
    }
  } catch (e) { /* 忽略 */ }
  let list = [];
  try { list = await invoke("get_sessions"); } catch (e) { return; }
  const tbody = $("#session-table tbody");
  // 行数与磁盘不一致(新增/删除会话)时整体重建;否则原位修补,避免打断 ⋯ 菜单
  const keys = new Set(list.map((s) => s.key));
  let stale = tbody.children.length !== list.length;
  if (!stale) {
    for (const tr of tbody.children) {
      if (!keys.has(tr.dataset.key)) { stale = true; break; }
    }
  }
  if (stale) { refreshSessions(); } else {
    for (const s of list) patchSessionRow(s);
  }
  // 详情页:状态 + 计数 + 事件/直播缓冲(渲染唯一入口)
  if (detailState.key) {
    const cur = list.find((x) => x.key === detailState.key);
    if (cur) {
      setDetailStatus(cur.status);
      $("#detail-meta").textContent =
        `${cur.count} 条消息 · 约 ${(cur.tokens || 0).toLocaleString()} tokens${cur.has_summary ? " · 含摘要" : ""}`;
    }
    await pollDetail();
  }
}
setInterval(pollLive, 1500);

// ---------- 初始化 ----------
loadConfig().catch((e) => {
  addLog("error", "初始化失败: " + e);
  $("#st-text").textContent = "加载失败";
});
