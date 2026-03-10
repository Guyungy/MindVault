const workspaceSelect = document.getElementById("workspace-select");
const workspaceSummary = document.getElementById("workspace-summary");
const taskSummary = document.getElementById("task-summary");
const relationList = document.getElementById("relation-list");
const taskList = document.getElementById("task-list");
const taskDetail = document.getElementById("task-detail");
const workspaceTitle = document.getElementById("workspace-title");
const workspaceDescription = document.getElementById("workspace-description");
const databaseTabs = document.getElementById("database-tabs");
const databaseTable = document.getElementById("database-table");
const tableTitle = document.getElementById("table-title");
const tableMeta = document.getElementById("table-meta");
const rowDetail = document.getElementById("row-detail");
const searchInput = document.getElementById("search-input");
const errorBanner = document.getElementById("error-banner");
const agentList = document.getElementById("agent-list");
const agentDetail = document.getElementById("agent-detail");
const navButtons = Array.from(document.querySelectorAll("[data-view]"));
const viewPanels = {
  databases: document.getElementById("view-databases"),
  tasks: document.getElementById("view-tasks"),
  agents: document.getElementById("view-agents"),
};

let currentPayload = null;
let activeDatabaseName = "";
let selectedRowIndex = -1;
let selectedTaskId = "";
let selectedAgentIndex = 0;
let activeView = "databases";

init();

async function init() {
  try {
    const workspaces = await fetchJson("/api/workspaces");
    const available = (workspaces.workspaces || []).filter((item) => item.hasMultiDb);
    renderWorkspaceOptions(available);
    if (workspaceSelect.value) {
      await loadWorkspace(workspaceSelect.value);
      return;
    }
    renderEmptyState("No workspace with multi_db output was found.");
  } catch (error) {
    showError(`Failed to load workspaces: ${error.message}`);
    renderEmptyState(`Failed to load workspaces: ${error.message}`);
  }
}

workspaceSelect.addEventListener("change", async (event) => {
  try {
    await loadWorkspace(event.target.value);
  } catch (error) {
    showError(`Failed to load workspace: ${error.message}`);
    renderEmptyState(`Failed to load workspace: ${error.message}`);
  }
});

searchInput.addEventListener("input", () => {
  renderActiveDatabase();
});

navButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setActiveView(button.dataset.view || "databases");
  });
});

async function loadWorkspace(workspaceId) {
  currentPayload = await fetchJson(`/api/workspaces/${encodeURIComponent(workspaceId)}`);
  const multiDb = currentPayload.multiDb || { databases: [], relations: [] };
  activeDatabaseName = multiDb.databases?.[0]?.name || "";
  selectedRowIndex = -1;
  selectedTaskId = currentPayload.latestTask?.task_id || "";
  selectedAgentIndex = 0;
  renderWorkspace();
}

function renderWorkspaceOptions(workspaces) {
  workspaceSelect.innerHTML = "";
  for (const workspace of workspaces) {
    const option = document.createElement("option");
    option.value = workspace.id;
    option.textContent = workspace.id;
    workspaceSelect.appendChild(option);
  }
}

function renderWorkspace() {
  const multiDb = currentPayload.multiDb || { databases: [], relations: [] };
  const databases = multiDb.databases || [];
  workspaceTitle.textContent = currentPayload.workspace || "Workspace";
  workspaceDescription.textContent = multiDb.domain || "No domain metadata.";

  renderSummary(databases, multiDb.relations || []);
  renderTask(currentPayload.latestTask);
  renderTasks(currentPayload.tasks || []);
  renderRelations(multiDb.relations || []);
  renderAgents(currentPayload.trace || []);
  renderDatabaseTabs(databases);
  renderActiveDatabase();
  setActiveView(activeView);
}

function renderSummary(databases, relations) {
  const rowCount = databases.reduce((sum, db) => sum + (db.rows?.length || 0), 0);
  const fieldCount = databases.reduce((sum, db) => sum + (db.columns?.length || 0), 0);
  const items = [
    ["Databases", databases.length],
    ["Rows", rowCount],
    ["Fields", fieldCount],
    ["Relations", relations.length],
  ];
  workspaceSummary.innerHTML = items
    .map(
      ([label, value]) =>
        `<div class="summary-item"><div class="summary-label">${escapeHtml(label)}</div><div class="summary-value">${escapeHtml(String(value))}</div></div>`,
    )
    .join("");
}

function renderRelations(relations) {
  relationList.innerHTML = relations.length
    ? relations
        .map(
          (relation) => `
          <div class="relation-item">
            <div><strong>${escapeHtml(relation.from_db || "")}</strong>.<span>${escapeHtml(relation.from_field || "")}</span></div>
            <div class="muted">${escapeHtml(relation.relation_type || "")}</div>
            <div><strong>${escapeHtml(relation.to_db || "")}</strong>.<span>${escapeHtml(relation.to_field || "")}</span></div>
          </div>
        `,
        )
        .join("")
    : `<div class="muted">No relations.</div>`;
}

function renderTask(task) {
  if (!task) {
    taskSummary.innerHTML = `<div class="muted">No task metadata.</div>`;
    return;
  }
  taskSummary.innerHTML = `
    <div class="task-item">
      <div class="task-item-header">
        <div><strong>${escapeHtml(task.task_id || "")}</strong></div>
        <div class="health-pill" data-health="${escapeHtml(task.monitor?.health || "unknown")}">${escapeHtml(task.monitor?.health || "unknown")}</div>
      </div>
      <div class="status-pill">${escapeHtml(task.status || "unknown")}</div>
      <div class="muted">Step: ${escapeHtml(task.current_step || "-")}</div>
      <div class="muted">Agent: ${escapeHtml(task.current_agent || "-")}</div>
      <div class="muted">Heartbeat: ${escapeHtml(task.last_heartbeat || "-")}</div>
      <div class="muted">Heartbeat Age: ${escapeHtml(formatAge(task.monitor?.heartbeat_age_seconds))}</div>
      <div class="muted">Resume: ${escapeHtml(task.resume_hint || "-")}</div>
    </div>
  `;
}

function renderTasks(tasks) {
  if (!tasks.length) {
    taskList.innerHTML = `<div class="muted">No tasks.</div>`;
    taskDetail.innerHTML = `<div class="muted">No task selected.</div>`;
    return;
  }

  taskList.innerHTML = tasks
    .map(
      (task) => `
        <div class="task-item buttonish${task.task_id === selectedTaskId ? " active" : ""}" data-task-id="${escapeHtml(task.task_id || "")}">
          <div class="task-item-header">
            <div><strong>${escapeHtml(task.task_id || "")}</strong></div>
            <div class="health-pill" data-health="${escapeHtml(task.monitor?.health || "unknown")}">${escapeHtml(task.monitor?.health || "unknown")}</div>
          </div>
          <div class="status-pill">${escapeHtml(task.status || "unknown")}</div>
          <div class="muted">Step: ${escapeHtml(task.current_step || "-")}</div>
          <div class="muted">Heartbeat: ${escapeHtml(task.last_heartbeat || "-")}</div>
        </div>
      `,
    )
    .join("");

  taskList.querySelectorAll("[data-task-id]").forEach((node) => {
    node.addEventListener("click", () => {
      selectedTaskId = node.dataset.taskId || "";
      renderTasks(tasks);
    });
  });

  const selectedTask = tasks.find((task) => task.task_id === selectedTaskId) || tasks[0];
  if (selectedTask && selectedTask.task_id !== selectedTaskId) {
    selectedTaskId = selectedTask.task_id;
  }
  taskDetail.innerHTML = renderTaskDetail(selectedTask);
}

function renderAgents(trace) {
  if (!trace.length) {
    agentList.innerHTML = `<div class="muted">No agent trace.</div>`;
    agentDetail.textContent = "No agent event selected.";
    return;
  }

  agentList.innerHTML = trace
    .map((entry, index) => {
      const label = entry.agent || entry.event || "event";
      const meta = entry.event || entry.task_type || "trace";
      return `
        <div class="agent-item${index === selectedAgentIndex ? " active" : ""}" data-agent-index="${index}">
          <div class="agent-item-top">
            <strong class="agent-name">${escapeHtml(label)}</strong>
            <span class="status-pill">${escapeHtml(meta)}</span>
          </div>
          <div class="muted">${escapeHtml(entry.timestamp || "-")}</div>
        </div>
      `;
    })
    .join("");

  agentList.querySelectorAll("[data-agent-index]").forEach((node) => {
    node.addEventListener("click", () => {
      selectedAgentIndex = Number(node.dataset.agentIndex);
      renderAgents(trace);
    });
  });

  const selectedEntry = trace[selectedAgentIndex] || trace[0];
  if (selectedEntry && trace[selectedAgentIndex] !== selectedEntry) {
    selectedAgentIndex = 0;
  }
  agentDetail.textContent = JSON.stringify(selectedEntry, null, 2);
}

function renderDatabaseTabs(databases) {
  databaseTabs.innerHTML = "";
  for (const database of databases) {
    const button = document.createElement("button");
    button.className = `tab-btn${database.name === activeDatabaseName ? " active" : ""}`;
    button.textContent = database.title || database.name;
    button.addEventListener("click", () => {
      activeDatabaseName = database.name;
      selectedRowIndex = -1;
      renderDatabaseTabs(databases);
      renderActiveDatabase();
    });
    databaseTabs.appendChild(button);
  }
}

function renderActiveDatabase() {
  if (!currentPayload) {
    renderEmptyState("No workspace loaded.");
    return;
  }
  const databases = currentPayload.multiDb?.databases || [];
  const activeDb = databases.find((db) => db.name === activeDatabaseName) || databases[0];
  if (!activeDb) {
    databaseTable.innerHTML = "";
    tableTitle.textContent = "No database";
    tableMeta.textContent = "";
    rowDetail.textContent = "No row selected.";
    return;
  }

  const rows = filterRows(activeDb.rows || [], searchInput.value || "");
  const columns = activeDb.columns || [];
  tableTitle.textContent = activeDb.title || activeDb.name;
  tableMeta.textContent = `${rows.length} rows · ${columns.length} fields`;

  const head = `<thead><tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead>`;
  const body = rows.length
    ? `<tbody>${rows
        .map((row, index) => {
          const active = index === selectedRowIndex ? " class=\"active\"" : "";
          return `<tr data-row-index="${index}"${active}>${columns
            .map((column) => `<td>${escapeHtml(stringifyValue(row[column]))}</td>`)
            .join("")}</tr>`;
        })
        .join("")}</tbody>`
    : `<tbody><tr><td colspan="${columns.length || 1}">No rows</td></tr></tbody>`;

  databaseTable.innerHTML = head + body;

  databaseTable.querySelectorAll("tbody tr[data-row-index]").forEach((rowEl) => {
    rowEl.addEventListener("click", () => {
      selectedRowIndex = Number(rowEl.dataset.rowIndex);
      rowDetail.textContent = JSON.stringify(rows[selectedRowIndex], null, 2);
      renderActiveDatabase();
    });
  });

  if (selectedRowIndex >= 0 && rows[selectedRowIndex]) {
    rowDetail.textContent = JSON.stringify(rows[selectedRowIndex], null, 2);
  } else {
    rowDetail.textContent = "No row selected.";
  }
}

function filterRows(rows, query) {
  const normalized = query.trim().toLowerCase();
  if (!normalized) return rows;
  return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(normalized));
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function stringifyValue(value) {
  if (Array.isArray(value) || (value && typeof value === "object")) {
    return JSON.stringify(value);
  }
  return value ?? "";
}

function escapeHtml(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderTaskDetail(task) {
  if (!task) return `<div class="muted">No task selected.</div>`;
  const metrics = [
    ["Current Step", task.current_step || "-"],
    ["Current Agent", task.current_agent || "-"],
    ["Heartbeat Age", formatAge(task.monitor?.heartbeat_age_seconds)],
    ["Recent Fallbacks", String(task.monitor?.recent_fallbacks ?? 0)],
    ["Recent Failures", String(task.monitor?.recent_failures ?? 0)],
    ["Recent Steps", String(task.monitor?.step_count ?? 0)],
  ];
  const steps = (task.recentSteps || [])
    .map(
      (step) => `
        <div class="task-step">
          <div class="task-step-top">
            <strong>${escapeHtml(step.action || "step")}</strong>
            <span class="status-pill">${escapeHtml(step.status || "unknown")}</span>
          </div>
          <div class="task-step-meta">
            <span>${escapeHtml(step.timestamp || "-")}</span>
            ${step.chunk_id ? `<span>chunk: ${escapeHtml(step.chunk_id)}</span>` : ""}
            ${step.output ? `<span>output: ${escapeHtml(step.output)}</span>` : ""}
            ${step.error ? `<span>error: ${escapeHtml(step.error)}</span>` : ""}
          </div>
        </div>
      `,
    )
    .join("");

  return `
    <div class="task-detail-meta">
      <div class="health-pill" data-health="${escapeHtml(task.monitor?.health || "unknown")}">${escapeHtml(task.monitor?.health || "unknown")}</div>
      <div class="status-pill">${escapeHtml(task.status || "unknown")}</div>
      <div class="muted">${escapeHtml(task.task_id || "")}</div>
    </div>
    <p class="muted">${escapeHtml(task.resume_hint || "-")}</p>
    <div class="task-detail-grid">
      ${metrics
        .map(
          ([label, value]) => `
            <div class="task-metric">
              <div class="task-metric-label">${escapeHtml(label)}</div>
              <div class="task-metric-value">${escapeHtml(value)}</div>
            </div>
          `,
        )
        .join("")}
    </div>
    <div class="task-step-list">
      ${steps || `<div class="muted">No recent steps.</div>`}
    </div>
  `;
}

function formatAge(seconds) {
  if (seconds == null) return "-";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
}

function renderEmptyState(message) {
  workspaceTitle.textContent = "Workspace Console";
  workspaceDescription.textContent = message;
  workspaceSummary.innerHTML = "";
  taskSummary.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
  relationList.innerHTML = `<div class="muted">No relations.</div>`;
  databaseTabs.innerHTML = "";
  databaseTable.innerHTML = "";
  tableTitle.textContent = "No database";
  tableMeta.textContent = "";
  rowDetail.textContent = "No row selected.";
  taskList.innerHTML = `<div class="muted">No tasks.</div>`;
  taskDetail.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
  if (agentList) {
    agentList.innerHTML = `<div class="muted">No agent trace.</div>`;
  }
  if (agentDetail) {
    agentDetail.textContent = message;
  }
}

function showError(message) {
  if (!errorBanner) return;
  errorBanner.textContent = message;
  errorBanner.classList.remove("hidden");
}

function setActiveView(viewName) {
  activeView = viewName;
  navButtons.forEach((button) => {
    button.classList.toggle("active", button.dataset.view === viewName);
  });
  Object.entries(viewPanels).forEach(([name, panel]) => {
    if (!panel) return;
    panel.classList.toggle("hidden", name !== viewName);
  });
}
