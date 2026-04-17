const state = {
  conversationId: localStorage.getItem("bankingAssistantConversationId"),
  chartCounter: 0,
};

const elements = {
  form: document.querySelector("#chatForm"),
  input: document.querySelector("#messageInput"),
  send: document.querySelector("#sendButton"),
  messages: document.querySelector("#messages"),
  health: document.querySelector("#healthStatus"),
};

function init() {
  renderWelcome();
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
}

async function checkHealth() {
  try {
    const response = await fetch("/health");
    const payload = await response.json();
    elements.health.textContent = payload.database_exists ? "Ready" : "Database missing";
    elements.health.className = payload.database_exists ? "health ok" : "health bad";
  } catch (error) {
    elements.health.textContent = "Offline";
    elements.health.className = "health bad";
  }
}

async function sendChat(message) {
  clearWelcome();
  appendMessage("user", message);
  const loading = appendMessage("assistant", "Thinking through governed metadata and business definitions...");
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
    loading.remove();
    renderAssistantResponse(payload);
  } catch (error) {
    loading.remove();
    appendMessage("assistant error", error.message || "The assistant request failed.");
  } finally {
    setBusy(false);
  }
}

function renderAssistantResponse(payload) {
  const bubble = document.createElement("article");
  bubble.className = "message assistant";
  bubble.appendChild(metaRow(payload));

  const answer = document.createElement("p");
  answer.textContent = payload.answer || "No answer was returned.";
  bubble.appendChild(answer);

  if (payload.requires_clarification) {
    const note = document.createElement("p");
    note.className = "hint";
    note.textContent = "Reply in the chat with the option you want me to use.";
    bubble.appendChild(note);
  }

  if (payload.chart_spec) renderChart(bubble, payload.chart_spec);
  if (payload.result_table) renderTable(bubble, payload.result_table);
  if (payload.generated_sql) renderDetails(bubble, "SQL Query", payload.generated_sql);
  if (payload.source_citations?.length) renderSources(bubble, payload.source_citations);

  elements.messages.appendChild(bubble);
  scrollToBottom();
}

function metaRow(payload) {
  const row = document.createElement("div");
  row.className = "meta";
  [payload.status, payload.intent, payload.route].filter(Boolean).forEach((item) => {
    const pill = document.createElement("span");
    pill.textContent = item;
    row.appendChild(pill);
  });
  return row;
}

function renderSources(container, sources) {
  const details = document.createElement("details");
  details.open = true;
  const summary = document.createElement("summary");
  summary.textContent = "Sources";
  const list = document.createElement("ul");
  sources.slice(0, 8).forEach((source) => {
    const item = document.createElement("li");
    const location = [source.table_name, source.column_name].filter(Boolean).join(".");
    item.textContent = `${source.source_type}: ${source.business_name || source.source_id}${location ? ` (${location})` : ""}`;
    list.appendChild(item);
  });
  details.appendChild(summary);
  details.appendChild(list);
  container.appendChild(details);
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
    th.textContent = column;
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

function renderWelcome() {
  elements.messages.innerHTML = `
    <div class="welcome" data-welcome="true">
      <p>Ask a governed commercial banking question.</p>
      <p>Examples: “What does average collected balance mean?”, “Show average deposit ledger balance by customer segment”, “Plot loan utilization by month”.</p>
    </div>
  `;
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

init();
