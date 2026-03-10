const workspaceSelect = document.getElementById("workspace-select");
const workspaceSummary = document.getElementById("workspace-summary");
const taskSummary = document.getElementById("task-summary");
const relationList = document.getElementById("relation-list");
const workspaceTitle = document.getElementById("workspace-title");
const workspaceDescription = document.getElementById("workspace-description");
const databaseTabs = document.getElementById("database-tabs");
const databaseTable = document.getElementById("database-table");
const tableTitle = document.getElementById("table-title");
const tableMeta = document.getElementById("table-meta");
const rowDetail = document.getElementById("row-detail");
const searchInput = document.getElementById("search-input");

let currentPayload = null;
let activeDatabaseName = "";
let selectedRowIndex = -1;

init();

async function init() {
  const workspaces = await fetchJson("/api/workspaces");
  renderWorkspaceOptions(workspaces.workspaces || []);
  if (workspaceSelect.value) {
    await loadWorkspace(workspaceSelect.value);
  }
}

workspaceSelect.addEventListener("change", async (event) => {
  await loadWorkspace(event.target.value);
});

searchInput.addEventListener("input", () => {
  renderActiveDatabase();
});

async function loadWorkspace(workspaceId) {
  currentPayload = await fetchJson(`/api/workspaces/${encodeURIComponent(workspaceId)}`);
  const multiDb = currentPayload.multiDb || { databases: [], relations: [] };
  activeDatabaseName = multiDb.databases?.[0]?.name || "";
  selectedRowIndex = -1;
  renderWorkspace();
}

function renderWorkspaceOptions(workspaces) {
  workspaceSelect.innerHTML = "";
  for (const workspace of workspaces.filter((item) => item.hasMultiDb)) {
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
  renderRelations(multiDb.relations || []);
  renderDatabaseTabs(databases);
  renderActiveDatabase();
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
      <div class="status-pill">${escapeHtml(task.status || "unknown")}</div>
      <div><strong>${escapeHtml(task.task_id || "")}</strong></div>
      <div class="muted">Step: ${escapeHtml(task.current_step || "-")}</div>
      <div class="muted">Agent: ${escapeHtml(task.current_agent || "-")}</div>
      <div class="muted">Heartbeat: ${escapeHtml(task.last_heartbeat || "-")}</div>
      <div class="muted">Resume: ${escapeHtml(task.resume_hint || "-")}</div>
    </div>
  `;
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
