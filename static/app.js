"use strict";

const state = {
  models: [],
  active: null,
  emails: [],
  jobs: [],
  expanded: new Set(),
  pollTimer: null,
};

const MAX_EMAILS = 16;

const SAMPLE_TEXTS = [
  "Congratulations! You are a winner. Claim your free prize now, click here.",
  "URGENT: your account will be suspended. Verify your account immediately.",
  "Hi team, attached is the quarterly report for your review before the meeting tomorrow.",
  "Limited offer! Invest in crypto today and double your money fast.",
  "Your parcel could not be delivered. Pay the small fee to reschedule.",
  "Final notice: send your password to unlock the refund.",
  "Can we schedule the sprint review? Thanks, regards.",
  "Please review the attached invoice from our vendor and approve the budget.",
];

// ---------------------------------------------------------------- utils
const $ = (sel) => document.querySelector(sel);
const el = (tag, cls, text) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined && text !== null) node.textContent = text;
  return node;
};
const fmt = (n, d = 3) =>
  n === null || n === undefined || Number.isNaN(n) ? "—" : Number(n).toFixed(d);
const esc = (s) => (s === null || s === undefined ? "" : String(s));

let toastTimer = null;
function toast(message, isError = false) {
  const node = $("#toast");
  node.textContent = message;
  node.classList.toggle("err", isError);
  node.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => node.classList.remove("show"), 3600);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const text = await response.text();
  let payload = null;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    payload = { detail: text };
  }
  if (!response.ok) {
    const detail =
      payload && payload.detail
        ? typeof payload.detail === "string"
          ? payload.detail
          : JSON.stringify(payload.detail)
        : `HTTP ${response.status}`;
    throw new Error(detail);
  }
  return payload;
}

function statusPill(status) {
  const map = {
    ok: ["ok", "成功"],
    timeout: ["timeout", "超时"],
    error: ["error", "错误"],
    accepted: ["pending", "已接单"],
    queued: ["pending", "排队"],
  };
  const [cls, label] = map[status] || ["pending", status || "—"];
  return el("span", `pill ${cls}`, label);
}

function versionPill(version) {
  if (!version) return el("span", "pill pending", "—");
  return el("span", `pill ${version === "v1" ? "v1" : "v2"}`, version);
}

// ---------------------------------------------------------------- emails
function newEmail(overrides = {}) {
  return {
    text: overrides.text ?? "",
    deadline_ms: overrides.deadline_ms ?? 500,
    submit_after_ms: overrides.submit_after_ms ?? 0,
    simulate_latency_ms: overrides.simulate_latency_ms ?? 0,
  };
}

function renderEmails() {
  const list = $("#email-list");
  list.textContent = "";
  state.emails.forEach((email, index) => {
    const row = el("div", "email-row");

    const top = el("div", "top");
    top.append(el("span", "idx", `#${index + 1}`));
    top.append(el("span", "small muted", "邮件正文"));
    top.append(el("span", "spacer"));
    const remove = el("button", "tiny danger", "移除");
    remove.onclick = () => {
      state.emails.splice(index, 1);
      if (!state.emails.length) state.emails.push(newEmail());
      renderEmails();
    };
    top.append(remove);
    row.append(top);

    const text = el("textarea");
    text.placeholder = "粘贴邮件正文…";
    text.value = email.text;
    text.oninput = () => {
      email.text = text.value;
      updateSubmitState();
    };
    row.append(text);

    const meta = el("div", "meta");

    const deadline = el("div", "field");
    deadline.append(el("label", null, `截止时间 · ${email.deadline_ms}ms`));
    const range = el("input");
    range.type = "range";
    range.min = "100";
    range.max = "1000";
    range.step = "10";
    range.value = String(email.deadline_ms);
    const out = el("output", null, `${email.deadline_ms}ms`);
    range.oninput = () => {
      email.deadline_ms = Number(range.value);
      out.textContent = `${email.deadline_ms}ms`;
      deadline.querySelector("label").textContent = `截止时间 · ${email.deadline_ms}ms`;
    };
    deadline.append(range);
    deadline.append(out);

    const stagger = el("div", "field");
    stagger.append(el("label", null, `错峰提交 · ${email.submit_after_ms}ms`));
    const staggerRange = el("input");
    staggerRange.type = "range";
    staggerRange.min = "0";
    staggerRange.max = "1000";
    staggerRange.step = "10";
    staggerRange.value = String(email.submit_after_ms);
    const staggerOut = el("output", null, `${email.submit_after_ms}ms`);
    staggerRange.oninput = () => {
      email.submit_after_ms = Number(staggerRange.value);
      staggerOut.textContent = `${email.submit_after_ms}ms`;
      stagger.querySelector("label").textContent = `错峰提交 · ${email.submit_after_ms}ms`;
    };
    stagger.append(staggerRange);
    stagger.append(staggerOut);

    meta.append(deadline, stagger);
    row.append(meta);

    const latency = el("div", "field");
    const label = el("label", null, "模拟推理延迟（毫秒，用于验证超时不改判成功）");
    const input = el("input");
    input.type = "number";
    input.min = "0";
    input.max = "5000";
    input.step = "50";
    input.value = String(email.simulate_latency_ms);
    input.oninput = () => {
      email.simulate_latency_ms = Math.max(0, Number(input.value) || 0);
    };
    latency.append(label, input);
    row.append(latency);

    list.append(row);
  });
  $("#submission-count").textContent = `${state.emails.length} / ${MAX_EMAILS}`;
  updateSubmitState();
}

function updateSubmitState() {
  const btn = $("#submit");
  btn.disabled = !state.emails.length || state.emails.length > MAX_EMAILS;
}

// ---------------------------------------------------------------- models
async function loadModels() {
  const data = await api("/api/models");
  state.models = data.models;
  state.active = data.active;
  renderModels();
  updateActiveBadge(data.active);
}

function updateActiveBadge(active) {
  const badge = $("#active-badge");
  badge.textContent = `现行模型 · ${active ?? "—"}`;
  badge.className = "badge active";
}

function renderModels() {
  const grid = $("#model-grid");
  grid.textContent = "";
  for (const model of state.models) {
    const card = el("div", "model-card" + (model.version === state.active ? " current" : ""));
    const row = el("div", "row");
    row.append(versionPill(model.version));
    row.append(el("span", "small muted", `阈值 ${model.threshold}`));
    row.append(el("span", "spacer"));
    if (model.version === state.active) {
      row.append(el("span", "badge active", "现行"));
    } else {
      const btn = el("button", "tiny primary", "切换到此版本");
      btn.onclick = () => switchModel(model.version);
      row.append(btn);
    }
    card.append(row);
    card.append(el("div", "desc", model.description || ""));
    card.append(el("div", "small muted", `词表 ${model.vocabulary_size} 项`));

    const terms = el("div", "terms");
    for (const term of model.top_spam_terms.slice(0, 6)) {
      terms.append(el("span", "term spam", `+${term.term} ${term.weight}`));
    }
    for (const term of model.top_ham_terms.slice(0, 4)) {
      terms.append(el("span", "term ham", `${term.term} ${term.weight}`));
    }
    card.append(terms);
    grid.append(card);
  }
  updateActiveBadge(state.active);
}

async function switchModel(version) {
  try {
    const data = await api("/api/model", {
      method: "POST",
      body: JSON.stringify({ version, note: "console switch" }),
    });
    state.active = data.active;
    toast(`现行模型已切换为 ${data.active}`);
    await loadModels();
    await refreshState();
  } catch (error) {
    toast(`切换失败：${error.message}`, true);
  }
}

// ---------------------------------------------------------------- state
async function refreshState() {
  let data;
  try {
    data = await api("/api/state");
  } catch (error) {
    return;
  }
  state.active = data.active_model;
  updateActiveBadge(data.active_model);
  const worker = data.worker;
  $("#pending-badge").textContent = `队列 ${worker.pending}`;
  $("#worker-config").textContent =
    `批 ≤${worker.max_batch_size} · 等待 ≤${worker.batch_wait_ms}ms · 提前 ${worker.dispatch_guard_ms}ms`;

  renderMetrics(worker);
  renderVersionMetrics(worker);
  renderBatches(worker.recent_batches);
}

function renderMetrics(worker) {
  const box = $("#metrics");
  box.textContent = "";
  const metrics = worker.metrics;
  const entries = [
    ["已接单", metrics.submitted],
    ["成功", metrics.completed_ok],
    ["超时", metrics.timed_out],
    ["错误", metrics.errored],
    ["批次数", metrics.batches],
    ["迟到丢弃", metrics.late_results_discarded],
    ["排队中", worker.pending],
  ];
  for (const [key, value] of entries) {
    const card = el("div", "metric");
    card.append(el("div", "k", key));
    card.append(el("div", "v", String(value ?? 0)));
    box.append(card);
  }
}

function renderVersionMetrics(worker) {
  const body = $("#version-metrics");
  body.textContent = "";
  const byVersion = worker.metrics_by_version || {};
  const versions = Object.keys(byVersion).sort();
  if (!versions.length) {
    const tr = el("tr");
    const td = el("td", "muted small", "暂无数据");
    td.colSpan = 5;
    tr.append(td);
    body.append(tr);
    return;
  }
  for (const version of versions) {
    const counts = byVersion[version];
    const tr = el("tr");
    const versionCell = el("td");
    versionCell.append(versionPill(version));
    tr.append(versionCell);
    for (const key of ["submitted", "completed_ok", "timed_out", "batches"]) {
      tr.append(el("td", "num", String(counts[key] ?? 0)));
    }
    body.append(tr);
  }
}

function renderBatches(batches) {
  const list = $("#batch-list");
  list.textContent = "";
  if (!batches || !batches.length) {
    list.append(el("div", "empty", "尚无批次"));
    return;
  }
  for (const batch of batches) {
    const row = el("div", "batch");
    row.append(versionPill(batch.model_version));
    row.append(el("span", "ids mono", `${batch.batch_id} · ${batch.request_ids.join(", ")}`));
    row.append(el("span", "small", `ok ${batch.ok} / 超时 ${batch.timeouts}`));
    row.append(el("span", "small muted mono", `${fmt(batch.duration_ms, 1)}ms`));
    list.append(row);
  }
}

// ---------------------------------------------------------------- jobs
function buildJobCard(job) {
  const card = el("div", "job");
  const head = el("div", "job-head");
  head.append(el("span", "mono small", job.id));
  head.append(el("span", "small", job.label || "未命名"));
  head.append(
    el(
      "span",
      "pill " +
        (job.status === "completed" ? "ok" : job.status === "failed" ? "error" : "pending"),
      job.status === "completed" ? "已完成" : job.status === "failed" ? "失败" : "进行中"
    )
  );
  head.append(el("span", "spacer"));
  for (const version of job.versions || []) head.append(versionPill(version));
  head.append(el("span", "small muted", `成功 ${job.ok} · 超时 ${job.timeouts}`));
  if (job.settled !== undefined) {
    head.append(el("span", "small muted", `${job.settled}/${job.size}`));
  }
  const toggle = el("button", "tiny ghost", state.expanded.has(job.id) ? "收起" : "展开");
  toggle.onclick = (event) => {
    event.stopPropagation();
    if (state.expanded.has(job.id)) state.expanded.delete(job.id);
    else state.expanded.add(job.id);
    refreshJobs(true);
  };
  head.append(toggle);
  card.append(head);

  const body = el("div", "job-body" + (state.expanded.has(job.id) ? "" : " collapsed"));
  if (job.events && job.events.length) {
    const events = el("div", "events");
    for (const event of job.events) {
      const label =
        event.type === "model_switched"
          ? `切换 ${event.from}→${event.to} @${event.at.slice(11, 19)}`
          : `接单 #${event.index} → ${event.model_version} @${event.at.slice(11, 19)}`;
      const cls = event.type === "model_switched" ? "event switch" : "event";
      const node = el("span", cls, label);
      if (event.pending_old !== undefined) {
        node.textContent += ` (旧版排队 ${event.pending_old})`;
      }
      events.append(node);
    }
    body.append(events);
  }

  const table = el("table");
  const thead = el("thead");
  const headRow = el("tr");
  for (const title of [
    "#",
    "正文",
    "截止",
    "状态",
    "分数",
    "分类",
    "模型",
    "批次",
    "延迟",
    "说明",
  ]) {
    headRow.append(el("th", null, title));
  }
  thead.append(headRow);
  table.append(thead);

  const tbody = el("tbody");
  const entries = job.entries || [];
  if (!entries.length) {
    const tr = el("tr");
    const td = el("td", "empty", "结果加载中…");
    td.colSpan = 10;
    tr.append(td);
    tbody.append(tr);
  }
  for (const entry of entries) {
    const tr = el("tr");
    tr.append(el("td", "num", String(entry.index)));
    const textCell = el("td", "text");
    const clamp = el("div", "clamp", entry.text);
    clamp.title = entry.text;
    textCell.append(clamp);
    tr.append(textCell);
    tr.append(el("td", "num", `${entry.deadline_ms}ms`));
    const statusCell = el("td");
    statusCell.append(statusPill(entry.status));
    tr.append(statusCell);
    tr.append(el("td", "num", fmt(entry.score, 4)));
    const labelCell = el("td");
    if (entry.label) {
      labelCell.append(
        el("span", "pill " + (entry.label === "spam" ? "timeout" : "ok"), entry.label)
      );
    } else {
      labelCell.append(el("span", "muted", "—"));
    }
    tr.append(labelCell);
    const versionCell = el("td");
    versionCell.append(versionPill(entry.model_version));
    tr.append(versionCell);
    tr.append(el("td", "mono small", entry.batch_id || "—"));
    tr.append(el("td", "num", entry.latency_ms === null || entry.latency_ms === undefined ? "—" : `${fmt(entry.latency_ms, 1)}ms`));
    const noteCell = el("td", "small muted");
    const notes = [];
    if (entry.reason) notes.push(entry.reason);
    if (entry.late_result_discarded) notes.push("迟到结果已丢弃");
    if (entry.simulate_latency_ms) notes.push(`模拟延迟 ${entry.simulate_latency_ms}ms`);
    if (entry.submit_after_ms) notes.push(`错峰 ${entry.submit_after_ms}ms`);
    noteCell.textContent = notes.join(" · ") || "—";
    tr.append(noteCell);
    tbody.append(tr);
  }
  table.append(tbody);
  body.append(table);
  card.append(body);
  return card;
}

async function refreshJobs(force = false) {
  let data;
  try {
    data = await api("/api/jobs?limit=12");
  } catch (error) {
    return;
  }
  state.jobs = data.jobs || [];
  const container = $("#jobs");
  container.textContent = "";
  if (!state.jobs.length) {
    container.append(el("div", "empty", "尚未提交任何邮件"));
    return;
  }
  for (const job of state.jobs) {
    container.append(buildJobCard(job));
  }
}

// ---------------------------------------------------------------- submission
async function submitEmails() {
  const emails = state.emails
    .filter((email) => email.text.trim().length > 0)
    .map((email) => ({
      text: email.text,
      deadline_ms: Number(email.deadline_ms),
      submit_after_ms: Number(email.submit_after_ms) || 0,
      simulate_latency_ms: Number(email.simulate_latency_ms) || 0,
    }));
  if (!emails.length) {
    toast("请至少填写一封邮件正文", true);
    return;
  }
  if (emails.length > MAX_EMAILS) {
    toast(`单次最多提交 ${MAX_EMAILS} 封`, true);
    return;
  }
  const button = $("#submit");
  button.disabled = true;
  try {
    const job = await api("/api/jobs", {
      method: "POST",
      body: JSON.stringify({ label: `手工提交 · ${emails.length} 封`, emails }),
    });
    state.expanded.add(job.id);
    toast(`已接单 ${job.size} 封，批次与版本见右侧`);
    await refreshJobs(true);
    await refreshState();
  } catch (error) {
    toast(`提交失败：${error.message}`, true);
  } finally {
    button.disabled = false;
    updateSubmitState();
  }
}

// ---------------------------------------------------------------- polling
function tickClock() {
  const now = new Date();
  $("#clock").textContent = now.toTimeString().slice(0, 8);
}

async function poll() {
  if ($("#auto-refresh").checked) {
    await Promise.all([refreshState(), refreshJobs()]);
  }
}

function startPolling() {
  tickClock();
  setInterval(tickClock, 1000);
  if (state.pollTimer) clearInterval(state.pollTimer);
  state.pollTimer = setInterval(poll, 900);
}

// ---------------------------------------------------------------- wiring
function fillDemo() {
  state.emails = SAMPLE_TEXTS.map((text, index) =>
    newEmail({
      text,
      deadline_ms: [900, 800, 700, 500, 400, 300, 600, 1000][index] ?? 500,
    })
  );
  renderEmails();
}

async function runDemo() {
  const button = $("#run-demo");
  button.disabled = true;
  try {
    const job = await api("/api/demo/interleaved", { method: "POST" });
    state.expanded.add(job.id);
    toast("交错演示已启动：先 v1 接单，切换 v2 后提交新请求");
    await refreshJobs(true);
    await refreshState();
  } catch (error) {
    toast(`演示失败：${error.message}`, true);
  } finally {
    button.disabled = false;
  }
}

function init() {
  fillDemo();
  $("#add-email").onclick = () => {
    if (state.emails.length >= MAX_EMAILS) {
      toast(`单次最多 ${MAX_EMAILS} 封`, true);
      return;
    }
    state.emails.push(newEmail());
    renderEmails();
  };
  $("#fill-demo").onclick = fillDemo;
  $("#clear-emails").onclick = () => {
    state.emails = [newEmail()];
    renderEmails();
  };
  $("#submit").onclick = submitEmails;
  $("#run-demo").onclick = runDemo;
  $("#refresh-models").onclick = loadModels;
  $("#auto-refresh").onchange = (event) => {
    if (!event.target.checked) return;
    refreshState();
    refreshJobs();
  };

  loadModels().catch((error) => toast(`加载模型失败：${error.message}`, true));
  refreshState();
  refreshJobs();
  startPolling();
}

document.addEventListener("DOMContentLoaded", init);
