const state = {
  conversationId: localStorage.getItem("bankingAssistantConversationId"),
  chartCounter: 0,
  recentConversations: loadRecentConversations(),
};

const elements = {
  form: document.querySelector("#chatForm"),
  input: document.querySelector("#messageInput"),
  send: document.querySelector("#sendButton"),
  messages: document.querySelector("#messages"),
  health: document.querySelector("#healthStatus"),
  llmStatus: document.querySelector("#llmStatus"),
  cacheStatus: document.querySelector("#cacheStatus"),
  recent: document.querySelector("#recentConversations"),
  newChat: document.querySelector("#newChatButton"),
};

function init() {
  renderWelcome();
  renderRecentConversations();
  bindControls();
  checkHealth();
}

function bindControls() {
  elements.form.addEventListener("submit", (event) => {
    event.preventDefault();
    const message = elements.input.value.trim();
    if (!message) return;
    elements.input.value = "";
    sendChat(message);
  });

  elements.newChat.addEventListener("click", () => {
    state.conversationId = null;
    localStorage.removeItem("bankingAssistantConversationId");
    renderWelcome();
    elements.input.focus();
  });
}

async function checkHealth() {
  const card = elements.health.closest(".status-card");
  try {
    const response = await fetch("/health");
    const payload = await response.json();
    elements.health.textContent = payload.database_exists ? "Connected" : "Database missing";
    card.classList.toggle("ok", payload.database_exists);
    card.classList.toggle("bad", !payload.database_exists);
  } catch (error) {
    elements.health.textContent = "Offline";
    card.classList.add("bad");
  }
}

async function sendChat(message) {
  clearWelcome();
  appendMessage("user", message);
  addRecentConversation(message);
  const loading = appendThinkingMessage();
  setBusy(true);

  try {
    const response = await fetch("/chat/message", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message,
        conversation_id: state.conversationId,
        user_role: "technical_user",
        technical_mode: true,
        execute_sql: true,
        limit: 100,
      }),
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `Request failed with ${response.status}`);
    state.conversationId = payload.conversation_id || state.conversationId;
    if (state.conversationId) {
      localStorage.setItem("bankingAssistantConversationId", state.conversationId);
    }
    updateRuntimeBadges(payload);
    removeThinkingMessage(loading);
    renderAssistantResponse(payload);
  } catch (error) {
    removeThinkingMessage(loading);
    appendMessage("assistant error", error.message || "The assistant request failed.");
  } finally {
    setBusy(false);
  }
}

function renderAssistantResponse(payload) {
  const bubble = document.createElement("article");
  bubble.className = "message assistant analyst-response";
  bubble.appendChild(metaRow(payload));

  renderAnswerSection(bubble, payload);

  if (payload.requires_clarification) {
    renderClarificationOptions(bubble, payload);
  }

  if (isAnalyticalPayload(payload)) renderSummaryCard(bubble, payload);
  if (payload.result_table) renderSection(bubble, "Result Table", (section) => renderTable(section, payload.result_table));
  if (payload.chart_spec) renderSection(bubble, "Chart", (section) => renderChart(section, payload.chart_spec));
  if (payload.generated_sql) renderSection(bubble, "SQL", (section) => renderDetails(section, "Generated Governed SQL", payload.generated_sql));
  if (payload.source_citations?.length) renderExpandableSection(bubble, "Sources", (section) => renderSources(section, payload.source_citations));
  renderExpandableSection(bubble, "Governance Audit", (section) => renderGovernanceAudit(section, payload));
  if (payload.turn_id) renderFeedback(bubble, payload);

  elements.messages.appendChild(bubble);
  scrollToBottom();
}

function metaRow(payload) {
  const row = document.createElement("div");
  row.className = "meta";
  [payload.status, payload.intent, payload.response_mode, payload.sql_validation?.is_valid ? "sql_validated" : null]
    .filter(Boolean)
    .forEach((item) => {
      const pill = document.createElement("span");
      pill.textContent = String(item).replaceAll("_", " ");
      row.appendChild(pill);
    });
  return row;
}

function renderAnswerSection(container, payload) {
  renderSection(container, "Answer", (section) => {
    const answer = document.createElement("p");
    answer.className = "answer-copy";
    answer.textContent = payload.answer || "No answer was returned.";
    section.appendChild(answer);
  });
}

function renderSummaryCard(container, payload) {
  const overview = payload.result_overview || {};
  const resultTable = payload.result_table || {};
  const metric = overview.metric || labelFromColumn(overview.metric_column) || "Governed metric";
  const dimensions = Array.isArray(overview.dimensions) && overview.dimensions.length
    ? overview.dimensions.join(", ")
    : labelFromColumn((resultTable.columns || [])[0]) || "Overall portfolio";

  renderSection(container, "Answer Summary", (section) => {
    const grid = document.createElement("div");
    grid.className = "summary-grid";
    [
      ["Metric", metric],
      ["Grouped By", dimensions],
      ["Rows Returned", resultTable.row_count ?? overview.row_count ?? "Not available"],
      ["Governance Status", payload.sql_validation?.is_valid ? "Validated" : payload.requires_clarification ? "Clarification needed" : "Metadata only"],
    ].forEach(([label, value]) => {
      const card = document.createElement("div");
      card.className = "summary-card";
      const caption = document.createElement("span");
      caption.textContent = label;
      const strong = document.createElement("strong");
      strong.textContent = value;
      card.appendChild(caption);
      card.appendChild(strong);
      grid.appendChild(card);
    });
    section.appendChild(grid);
  });
}

function renderSection(container, title, renderBody) {
  const section = document.createElement("section");
  section.className = `response-section ${title.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;
  const heading = document.createElement("div");
  heading.className = "section-heading";
  heading.textContent = title;
  section.appendChild(heading);
  renderBody(section);
  container.appendChild(section);
}

function renderExpandableSection(container, title, renderBody) {
  const details = document.createElement("details");
  details.className = `response-section expandable-section ${title.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`;
  const summary = document.createElement("summary");
  summary.className = "section-heading";
  summary.textContent = title;
  const body = document.createElement("div");
  body.className = "expandable-body";
  renderBody(body);
  details.appendChild(summary);
  details.appendChild(body);
  container.appendChild(details);
}

function renderClarificationOptions(container, payload) {
  renderSection(container, "Clarification", (section) => {
    const intro = document.createElement("p");
    intro.className = "hint";
    intro.textContent = "Select one governed option or reply in the chat with the choice you want me to use.";
    section.appendChild(intro);

    const groups = payload.clarification_options || [];
    groups.forEach((group) => {
      const groupTitle = document.createElement("p");
      groupTitle.className = "clarification-question";
      groupTitle.textContent = group.question || "Which governed option should I use?";
      section.appendChild(groupTitle);

      const choices = document.createElement("div");
      choices.className = "choice-grid";
      (group.options || []).forEach((option, index) => {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "choice-card";
        const location = [option.table, option.column].filter(Boolean).join(".");
        button.innerHTML = `
          <span>${index + 1}</span>
          <strong>${escapeHtml(option.label || option.id || "Governed option")}</strong>
          ${location ? `<small>${escapeHtml(location)}</small>` : ""}
        `;
        button.addEventListener("click", () => {
          sendChat(option.label || option.id || String(index + 1));
        });
        choices.appendChild(button);
      });
      section.appendChild(choices);
    });
  });
}

function renderSources(container, sources) {
  const list = document.createElement("div");
  list.className = "source-list";
  sources.slice(0, 8).forEach((source) => {
    const item = document.createElement("div");
    item.className = "source-card";
    const location = [source.table_name, source.column_name].filter(Boolean).join(".");
    const score = typeof source.score === "number" ? `${Math.round(source.score * 100)}% match` : "";
    item.innerHTML = `
      <span>${escapeHtml(source.source_type || "source")}</span>
      <strong>${escapeHtml(source.business_name || source.source_id || "Governed metadata")}</strong>
      ${location ? `<small>${escapeHtml(location)}</small>` : ""}
      ${score ? `<em>${score}</em>` : ""}
    `;
    list.appendChild(item);
  });
  container.appendChild(list);
}

function renderGovernanceAudit(container, payload) {
  const audit = payload.audit_report || {};
  const validation = payload.sql_validation || {};
  const trace = payload.llm_trace || {};
  const items = [
    ["SQL Validation", validation.is_valid ? "Passed" : payload.generated_sql ? "Blocked" : "Not required"],
    ["Generated SQL", payload.generated_sql ? "Available to technical user" : "Not generated"],
    ["Source Citations", `${payload.source_citations?.length || 0} cited`],
    ["LLM Policy", trace.policy_reason || "Response grounded in governed metadata"],
    ["Cache", trace.cache_status ? `${trace.cache_status}${trace.cache_backend ? ` via ${trace.cache_backend}` : ""}` : "Not used"],
  ];

  const grid = document.createElement("div");
  grid.className = "audit-grid";
  items.forEach(([label, value]) => {
    const item = document.createElement("div");
    item.className = "audit-item";
    item.innerHTML = `<span>${escapeHtml(label)}</span><strong>${escapeHtml(String(value))}</strong>`;
    grid.appendChild(item);
  });
  container.appendChild(grid);

  if (audit.summary || audit.status) {
    const note = document.createElement("p");
    note.className = "hint";
    note.textContent = audit.summary || `Audit status: ${audit.status}`;
    container.appendChild(note);
  }
}

function renderFeedback(container, payload) {
  const feedback = document.createElement("div");
  feedback.className = "feedback";

  const label = document.createElement("span");
  label.textContent = "Was this useful?";
  feedback.appendChild(label);

  const positive = feedbackButton("positive", "Helpful", "Mark answer as helpful");
  const negative = feedbackButton("negative", "Needs review", "Mark answer as needing review");
  feedback.appendChild(positive);
  feedback.appendChild(negative);

  const status = document.createElement("span");
  status.className = "feedback-status";
  feedback.appendChild(status);

  [positive, negative].forEach((button) => {
    button.addEventListener("click", () => {
      sendFeedback(payload, button.dataset.rating, feedback, status);
    });
  });

  container.appendChild(feedback);
}

function feedbackButton(rating, text, label) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "feedback-button";
  button.dataset.rating = rating;
  button.textContent = text;
  button.title = label;
  button.setAttribute("aria-label", label);
  return button;
}

async function sendFeedback(payload, rating, feedback, status) {
  if (feedback.dataset.submitted === rating) return;
  const buttons = feedback.querySelectorAll("button");
  buttons.forEach((button) => {
    button.disabled = true;
  });
  status.textContent = "Sending";

  try {
    const response = await fetch("/feedback", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        turn_id: payload.turn_id,
        conversation_id: payload.conversation_id,
        rating,
        reason_code: rating === "positive" ? "helpful" : "other",
        user_role: "technical_user",
      }),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.detail || `Feedback failed with ${response.status}`);
    feedback.dataset.submitted = rating;
    buttons.forEach((button) => {
      button.classList.toggle("selected", button.dataset.rating === rating);
      button.disabled = false;
    });
    status.textContent = "Saved";
  } catch (error) {
    buttons.forEach((button) => {
      button.disabled = false;
    });
    status.textContent = "Could not save";
  }
}

function renderChart(container, chartSpec) {
  const chart = document.createElement("div");
  chart.className = "chart";
  chart.id = `chart-${state.chartCounter}`;
  state.chartCounter += 1;
  container.appendChild(chart);

  queueMicrotask(() => {
    if (!window.Plotly) {
      chart.textContent = "Chart library could not be loaded.";
      return;
    }
    const figure = chartSpec.plotly_json || {};
    window.Plotly.newPlot(chart.id, figure.data || [], figure.layout || {}, {
      displayModeBar: false,
      responsive: true,
    });
  });
}

function renderTable(container, resultTable) {
  const wrapper = document.createElement("div");
  wrapper.className = "table-wrap";
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const body = document.createElement("tbody");
  const header = document.createElement("tr");

  resultTable.columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = labelFromColumn(column);
    header.appendChild(th);
  });
  head.appendChild(header);

  resultTable.rows.slice(0, 25).forEach((row) => {
    const tr = document.createElement("tr");
    resultTable.columns.forEach((column) => {
      const td = document.createElement("td");
      td.textContent = formatValue(row[column]);
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });

  table.appendChild(head);
  table.appendChild(body);
  wrapper.appendChild(table);
  container.appendChild(wrapper);
}

function renderDetails(container, title, text) {
  const details = document.createElement("details");
  details.open = true;
  const summary = document.createElement("summary");
  const pre = document.createElement("pre");
  summary.textContent = title;
  pre.textContent = text;
  details.appendChild(summary);
  details.appendChild(pre);
  container.appendChild(details);
}

function appendMessage(type, text) {
  const bubble = document.createElement("article");
  bubble.className = `message ${type}`;
  const paragraph = document.createElement("p");
  paragraph.textContent = text;
  bubble.appendChild(paragraph);
  elements.messages.appendChild(bubble);
  scrollToBottom();
  return bubble;
}

function appendThinkingMessage() {
  const bubble = document.createElement("article");
  bubble.className = "message assistant thinking";
  const title = document.createElement("p");
  title.className = "thinking-title";
  title.textContent = "Working through governed analytics";
  bubble.appendChild(title);

  const steps = [
    "Retrieving governed metadata",
    "Resolving certified metrics and dimensions",
    "Generating and validating SQL",
    "Preparing answer, evidence, and chart",
  ];
  const list = document.createElement("div");
  list.className = "thinking-steps";
  steps.forEach((step, index) => {
    const item = document.createElement("span");
    item.textContent = step;
    if (index === 0) item.classList.add("active");
    list.appendChild(item);
  });
  bubble.appendChild(list);
  elements.messages.appendChild(bubble);
  scrollToBottom();

  let activeIndex = 0;
  const timer = window.setInterval(() => {
    const children = [...list.children];
    children[activeIndex]?.classList.remove("active");
    children[activeIndex]?.classList.add("done");
    activeIndex = Math.min(activeIndex + 1, children.length - 1);
    children[activeIndex]?.classList.add("active");
  }, 900);
  bubble.stopThinking = () => window.clearInterval(timer);
  return bubble;
}

function removeThinkingMessage(bubble) {
  if (typeof bubble.stopThinking === "function") bubble.stopThinking();
  bubble.remove();
}

function renderWelcome() {
  const prompts = [
    "What does average collected balance mean?",
    "Plot loan utilization by month.",
    "Show total deposit transaction amount by channel.",
    "Create a bar chart of relationship profit by customer segment.",
  ];
  elements.messages.innerHTML = `
    <div class="welcome" data-welcome="true">
      <p>Ask a governed enterprise data question.</p>
      <p>Get business definitions, validated SQL, source citations, result tables, and executive-ready charts.</p>
      <div class="prompt-grid">
        ${prompts.map((prompt) => `<button type="button" data-prompt="${escapeHtml(prompt)}">${escapeHtml(prompt)}</button>`).join("")}
      </div>
    </div>
  `;
  elements.messages.querySelectorAll("[data-prompt]").forEach((button) => {
    button.addEventListener("click", () => sendChat(button.dataset.prompt));
  });
}

function clearWelcome() {
  const welcome = elements.messages.querySelector("[data-welcome]");
  if (welcome) welcome.remove();
}

function setBusy(isBusy) {
  elements.send.disabled = isBusy;
  elements.input.disabled = isBusy;
}

function formatValue(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") {
    if (Math.abs(value) < 1 && value !== 0) return `${(value * 100).toFixed(2)}%`;
    return new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 }).format(value);
  }
  return String(value);
}

function scrollToBottom() {
  elements.messages.scrollTop = elements.messages.scrollHeight;
}

function isAnalyticalPayload(payload) {
  return Boolean(payload.result_table || payload.generated_sql || payload.chart_spec || payload.result_overview?.metric);
}

function labelFromColumn(column) {
  if (!column) return "";
  return String(column)
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function updateRuntimeBadges(payload) {
  if (payload.llm_trace?.provider) {
    elements.llmStatus.textContent = payload.llm_trace.provider.includes("bedrock") ? "Bedrock" : "Fallback";
  }
  if (payload.llm_trace?.cache_status) {
    elements.cacheStatus.textContent = payload.llm_trace.cache_status === "hit" ? "Hit" : "Stored";
  }
}

function loadRecentConversations() {
  try {
    return JSON.parse(localStorage.getItem("bankingAssistantRecentConversations") || "[]");
  } catch (error) {
    return [];
  }
}

function saveRecentConversations() {
  localStorage.setItem("bankingAssistantRecentConversations", JSON.stringify(state.recentConversations.slice(0, 8)));
}

function addRecentConversation(message) {
  const normalized = message.toLowerCase().trim();
  state.recentConversations = [
    { message, createdAt: new Date().toISOString() },
    ...state.recentConversations.filter((item) => item.message.toLowerCase().trim() !== normalized),
  ].slice(0, 8);
  saveRecentConversations();
  renderRecentConversations();
}

function renderRecentConversations() {
  if (!elements.recent) return;
  if (!state.recentConversations.length) {
    elements.recent.innerHTML = `<p class="empty-recent">Recent questions will appear here after you start chatting.</p>`;
    return;
  }
  elements.recent.innerHTML = "";
  state.recentConversations.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "recent-item";
    button.textContent = item.message;
    button.addEventListener("click", () => sendChat(item.message));
    elements.recent.appendChild(button);
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

init();
