// LightBot 前端逻辑(零构建,Tauri v2 withGlobalTauri)

const T = window.__TAURI__;
const invoke = T ? T.core.invoke : async () => { throw new Error("请在 Tauri 桌面环境中运行"); };
const listen = T ? T.event.listen : async () => {};

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

// ---------- 全局状态 ----------
let cfg = null;           // 当前配置副本
let running = false;      // 机器人运行中
let stats = { hit: 0, miss: 0, last: null }; // 缓存统计累计
const logs = [];          // 日志行 {ts, level, msg}

// ---------- 主题(亮/暗切换,记住选择;首次跟随系统) ----------
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

// ---------- 面板切换 ----------
$$("#sidebar nav a").forEach((a) => {
  a.addEventListener("click", () => {
    $$("#sidebar nav a").forEach((x) => x.classList.remove("active"));
    a.classList.add("active");
    $$(".tab").forEach((t) => t.classList.remove("active"));
    $("#tab-" + a.dataset.tab).classList.add("active");
    // 进入会话设置时主动刷新(不依赖事件链路,事件丢失也能看到最新)
    if (a.dataset.tab === "chat") {
      refreshSessions();
      renderMemories();
    }
  });
});

// ---------- 日志 ----------
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

function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ---------- 事件流 ----------
listen("frontend", (e) => handleEvent(e.payload));

function handleEvent(ev) {
  switch (ev.type) {
    case "log":
      addLog(ev.level, ev.msg);
      break;
    case "notice":
      addLog("notice", `🔔 [${ev.notice_type}] ${ev.desc}`);
      break;
    case "msg_in":
      addLog("msg_in", `← ${ev.key}: ${ev.text}`);
      break;
    case "msg_out":
      addLog("msg_out", `→ ${ev.key}: ${ev.text}`);
      break;
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
      break;
    }
    case "session_changed":
      refreshSessions();
      break;
    case "status":
      updateStatusView(ev.status);
      break;
  }
}

// ---------- 状态显示 ----------
function updateStatusView(st) {
  const dot = $("#st-dot");
  const txt = $("#st-text");
  if (!running) { dot.className = "dot gray"; txt.textContent = "未启动"; }
  else if (st.connected) { dot.className = "dot green"; txt.textContent = "已连接 NapCat"; }
  else { dot.className = "dot red"; txt.textContent = "未连接"; }

  $("#ov-running").textContent = running ? "运行中" : "未启动";
  $("#ov-conn").textContent = running ? (st.connected ? "已连接" : `未连接${st.last_error ? " · " + st.last_error : ""}`) : "-";
  $("#ov-mode").textContent = st.mode === "reverse" ? "反向 WS" : "正向 WS";
  $("#ov-endpoint").textContent = st.endpoint || "-";
  $("#ov-self").textContent = st.self_id ? String(st.self_id) : (cfg?.napcat?.self_id || "自动获取中");
}

function renderOverview() {
  const total = stats.hit + stats.miss;
  $("#ov-hit-ratio").textContent = total > 0 ? Math.round((stats.hit / total) * 100) + "%" : "-";
  $("#ov-hit").textContent = stats.hit > 0 ? stats.hit.toLocaleString() + " tokens" : "-";
  $("#ov-miss").textContent = stats.miss > 0 ? stats.miss.toLocaleString() + " tokens" : "-";
  $("#ov-last-ms").textContent = stats.last ? `${stats.last.elapsed_ms} ms` : "-";
}

// ---------- 配置读取与表单绑定 ----------
async function loadConfig() {
  cfg = await invoke("get_config");
  bindConfigToForm();
  renderModels();
  renderPrompts();
  renderOverview();
  const sv = await invoke("get_status_view");
  running = sv.running;
  updateStatusView({ connected: sv.connected, mode: sv.mode, endpoint: sv.endpoint, self_id: sv.self_id, last_error: sv.last_error });
  $("#btn-toggle").textContent = running ? "停止" : "启动";
  await refreshSessions();
  await renderMemories();
}

function bindConfigToForm() {
  const n = cfg.napcat, c = cfg.chat, ij = c.interject;
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
  $("#cfg-summarize").checked = c.summarize;
  $("#cfg-summarize-tokens").value = c.summarize_tokens;
  $("#cfg-clean-hours").value = c.clean_after_hours;
  $("#cfg-est-ratio").value = c.estimate_ratio;
  $("#cfg-interject").checked = ij.enabled;
  $("#cfg-interject-mode").value = ij.mode;
  $("#cfg-interject-cooldown").value = ij.cooldown_secs;
  $("#cfg-interject-prob").value = Math.round(ij.base_probability * 100);
  $("#cfg-interject-maxtok").value = ij.interject_max_tokens;
  $("#cfg-interject-window").value = ij.activity_window_minutes;
  $("#cfg-soft-at").checked = ij.soft_at_reply;
  $("#cfg-names").value = ij.names;
  $("#cfg-hooks").value = ij.hooks;
  $("#cfg-memory").checked = c.memory.enabled;
  $("#cfg-mem-max").value = c.memory.max_entries;
  $("#cfg-mem-chars").value = c.memory.max_entry_chars;
  $("#cfg-mem-tokens").value = c.memory.max_tokens;
  syncModeFields();
}

function collectForm() {
  const n = cfg.napcat, c = cfg.chat, ij = c.interject;
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
  c.context_tokens = parseInt($("#cfg-context").value) || 8192;
  c.reserve_tokens = parseInt($("#cfg-reserve").value) || 1024;
  c.summarize = $("#cfg-summarize").checked;
  c.summarize_tokens = parseInt($("#cfg-summarize-tokens").value) || 600;
  c.clean_after_hours = parseInt($("#cfg-clean-hours").value) || 0;
  c.estimate_ratio = parseFloat($("#cfg-est-ratio").value) || 1.15;
  ij.enabled = $("#cfg-interject").checked;
  ij.mode = $("#cfg-interject-mode").value;
  ij.cooldown_secs = parseInt($("#cfg-interject-cooldown").value) || 90;
  ij.base_probability = (parseFloat($("#cfg-interject-prob").value) || 5) / 100;
  ij.interject_max_tokens = parseInt($("#cfg-interject-maxtok").value) || 120;
  ij.activity_window_minutes = parseInt($("#cfg-interject-window").value) || 2;
  ij.soft_at_reply = $("#cfg-soft-at").checked;
  ij.names = $("#cfg-names").value.trim();
  ij.hooks = $("#cfg-hooks").value.trim();
  c.memory.enabled = $("#cfg-memory").checked;
  c.memory.max_entries = parseInt($("#cfg-mem-max").value) || 30;
  c.memory.max_entry_chars = parseInt($("#cfg-mem-chars").value) || 200;
  c.memory.max_tokens = parseInt($("#cfg-mem-tokens").value) || 1200;
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
      cfg.prompts.splice(i, 1);
      if (cfg.active_prompt === cfg.prompts[i]?.id) cfg.active_prompt = cfg.prompts[0].id;
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

// ---------- 会话列表 ----------
async function refreshSessions() {
  const tbody = $("#session-table tbody");
  tbody.innerHTML = "";
  let list = [];
  try { list = await invoke("get_sessions"); } catch (e) { /* 未运行 */ }
  for (const s of list) {
    const tr = document.createElement("tr");
    const kind = s.key.startsWith("g") ? "群" : "私聊";
    tr.innerHTML = `
      <td>${kind} ${escapeHtml(s.key.slice(1))}</td>
      <td>${s.count}</td>
      <td>${s.tokens}</td>
      <td>${s.has_summary ? "✓" : "-"}</td>
      <td><button class="btn small danger clear-session">清空</button></td>`;
    tr.querySelector(".clear-session").addEventListener("click", async () => {
      try {
        await invoke("clear_session", { key: s.key });
        refreshSessions();
      } catch (e) { addLog("error", "清空失败: " + e); }
    });
    tbody.appendChild(tr);
  }
}

// ---------- 记忆管理 ----------
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

// ---------- 初始化 ----------
loadConfig().catch((e) => {
  addLog("error", "初始化失败: " + e);
  $("#st-text").textContent = "加载失败";
});
