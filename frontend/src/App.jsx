import { useEffect, useMemo, useState } from "react";
import { Activity, Database, GitBranch, PanelLeft, Search } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

const views = [
  { id: "databases", label: "Databases", icon: Database },
  { id: "tasks", label: "Tasks", icon: Activity },
  { id: "agents", label: "Agents", icon: GitBranch },
];

export default function App() {
  const [workspaces, setWorkspaces] = useState([]);
  const [workspaceId, setWorkspaceId] = useState("");
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState("");
  const [activeView, setActiveView] = useState("databases");
  const [activeDatabase, setActiveDatabase] = useState("");
  const [selectedRowIndex, setSelectedRowIndex] = useState(-1);
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [selectedAgentIndex, setSelectedAgentIndex] = useState(0);
  const [query, setQuery] = useState("");
  const [databaseRows, setDatabaseRows] = useState({});
  const [rowForm, setRowForm] = useState({});
  const [chatMessages, setChatMessages] = useState([]);
  const [chatInput, setChatInput] = useState("");

  useEffect(() => {
    loadWorkspaces();
  }, []);

  async function loadWorkspaces() {
    try {
      setError("");
      const result = await fetchJson("/api/workspaces");
      const available = (result.workspaces || []).filter((item) => item.hasMultiDb);
      setWorkspaces(available);
      if (available[0]) {
        setWorkspaceId(available[0].id);
      } else {
        setError("No workspace with multi_db output was found.");
      }
    } catch (err) {
      setError(err.message);
    }
  }

  useEffect(() => {
    if (!workspaceId) return;
    loadWorkspace(workspaceId);
  }, [workspaceId]);

  async function loadWorkspace(id) {
    try {
      setError("");
      const result = await fetchJson(`/api/workspaces/${encodeURIComponent(id)}`);
      setPayload(result);
      setActiveDatabase(result.multiDb?.databases?.[0]?.name || "");
      setSelectedRowIndex(-1);
      setSelectedTaskId(result.latestTask?.task_id || "");
      setSelectedAgentIndex(0);
      const newRows = {};
      (result.multiDb?.databases || []).forEach((db) => {
        newRows[db.name] = db.rows ? [...db.rows] : [];
      });
      setDatabaseRows(newRows);
    } catch (err) {
      setError(err.message);
    }
  }

  const databases = payload?.multiDb?.databases || [];
  const relations = payload?.multiDb?.relations || [];
  const tasks = payload?.tasks || [];
  const trace = payload?.trace || [];

  const activeDb = databases.find((db) => db.name === activeDatabase) || databases[0];
  const filteredRows = useMemo(() => {
    const rows = databaseRows[activeDb?.name] || [];
    if (!query.trim()) return rows;
    return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(query.trim().toLowerCase()));
  }, [activeDb, query, databaseRows]);
  const selectedRow = filteredRows[selectedRowIndex] || null;
  const selectedTask = tasks.find((task) => task.task_id === selectedTaskId) || tasks[0] || null;
  const selectedAgent = trace[selectedAgentIndex] || trace[0] || null;

  useEffect(() => {
    if (selectedRow) {
      setRowForm(selectedRow);
    } else {
      setRowForm({});
    }
  }, [selectedRow]);

  return (
    <div className="min-h-screen bg-[linear-gradient(180deg,#faf7f2_0%,#f2ebe2_100%)] text-stone-900">
      {error ? <div className="sticky top-0 z-50 bg-rose-900 px-4 py-3 text-sm text-rose-50">{error}</div> : null}
      <div className="grid min-h-screen lg:grid-cols-[320px_1fr]">
          <aside className="bg-white/60 p-6 backdrop-blur-sm">
            <div className="mb-7 space-y-1">
              <p className="text-xs uppercase tracking-[0.32em] text-stone-500">MindVault</p>
              <h1 className="text-2xl font-semibold text-stone-900">Control Console</h1>
            </div>

            <label className="block text-xs uppercase tracking-[0.16em] text-stone-500">Workspace</label>
            <select
              className="mb-6 w-full rounded-2xl border border-stone-200 bg-white px-3 py-2 text-sm outline-none transition focus:border-teal-600"
              value={workspaceId}
              onChange={(event) => setWorkspaceId(event.target.value)}
            >
              {workspaces.map((workspace) => (
                <option key={workspace.id} value={workspace.id}>
                  {workspace.id}
                </option>
              ))}
            </select>

            <div className="flex flex-col gap-2">
              {views.map((view) => {
                const Icon = view.icon;
                return (
                  <Button
                    key={view.id}
                    variant={activeView === view.id ? "activeNav" : "nav"}
                    onClick={() => setActiveView(view.id)}
                  >
                    <Icon className="h-4 w-4" />
                    {view.label}
                  </Button>
                );
              })}
            </div>
          </aside>

        <main className="p-6 lg:p-8">
          <header className="mb-6 flex flex-col gap-4 rounded-[28px] border border-stone-200 bg-white/80 p-6 shadow-sm backdrop-blur lg:flex-row lg:items-end lg:justify-between">
            <div>
              <p className="text-xs uppercase tracking-[0.24em] text-teal-700">Workspace</p>
              <h2 className="mt-2 text-3xl font-semibold">{payload?.workspace || "Loading"}</h2>
              <p className="mt-2 max-w-2xl text-sm text-stone-500">{payload?.multiDb?.domain || "No domain metadata."}</p>
            </div>
            <label className="flex w-full max-w-sm items-center gap-3 rounded-2xl border border-stone-200 bg-white px-4 py-3 shadow-sm">
              <Search className="h-4 w-4 text-stone-400" />
              <input
                className="w-full bg-transparent text-sm outline-none placeholder:text-stone-400"
                placeholder="搜索当前数据库..."
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </label>
          </header>

          {activeView === "databases" ? (
            <DatabasesView
              databases={databases}
              activeDatabase={activeDatabase}
              onActiveDatabaseChange={setActiveDatabase}
              filteredRows={filteredRows}
              selectedRow={selectedRow}
              selectedRowIndex={selectedRowIndex}
              onSelectRow={setSelectedRowIndex}
            />
          ) : null}

          {activeView === "tasks" ? (
            <TasksView
              tasks={tasks}
              selectedTask={selectedTask}
              selectedTaskId={selectedTaskId}
              onSelectTask={setSelectedTaskId}
            />
          ) : null}

          {activeView === "agents" ? (
            <AgentsView
              trace={trace}
              selectedAgent={selectedAgent}
              selectedAgentIndex={selectedAgentIndex}
              onSelectAgent={setSelectedAgentIndex}
            />
          ) : null}
        </main>
      </div>
    </div>
  );
}

function DatabasesView({
  databases,
  activeDatabase,
  onActiveDatabaseChange,
  filteredRows,
  selectedRow,
  selectedRowIndex,
  onSelectRow,
}) {
  const db = databases.find((item) => item.name === activeDatabase) || databases[0];

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <PanelLeft className="h-4 w-4" />
            Databases
          </CardTitle>
          <CardDescription>用 Tabs 切换数据库，字段与行都是动态结构。</CardDescription>
        </CardHeader>
        <CardContent>
          <Tabs value={db?.name} onValueChange={onActiveDatabaseChange}>
            <TabsList>
              {databases.map((database) => (
                <TabsTrigger key={database.name} value={database.name}>
                  {database.title || database.name}
                </TabsTrigger>
              ))}
            </TabsList>
            {databases.map((database) => (
              <TabsContent key={database.name} value={database.name}>
                <div className="grid gap-6 xl:grid-cols-[1.45fr_0.9fr]">
                  <Card className="overflow-hidden">
                    <CardHeader>
                      <CardTitle>{database.title || database.name}</CardTitle>
                      <CardDescription>
                        {(database.rows || []).length} rows · {(database.columns || []).length} fields
                      </CardDescription>
                    </CardHeader>
                    <CardContent>
                      <ScrollArea className="h-[560px] rounded-2xl border border-stone-200">
                        <table className="w-full border-collapse text-sm">
                          <thead className="sticky top-0 bg-stone-100 text-stone-700">
                            <tr>
                              {(database.columns || []).map((column) => (
                                <th key={column} className="border-b border-stone-200 px-4 py-3 text-left font-medium">
                                  {column}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {filteredRows.map((row, index) => (
                              <tr
                                key={`${database.name}-${index}`}
                                className={index === selectedRowIndex ? "bg-teal-50" : "odd:bg-white even:bg-stone-50"}
                                onClick={() => onSelectRow(index)}
                              >
                                {(database.columns || []).map((column) => (
                                  <td key={`${column}-${index}`} className="border-b border-stone-100 px-4 py-3 align-top">
                                    <div className="max-w-[260px] truncate">{stringifyValue(row[column])}</div>
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </ScrollArea>
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader>
                      <CardTitle>Row Detail</CardTitle>
                      <CardDescription>点击任意行查看完整 JSON。</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <pre className="h-[560px] overflow-auto rounded-2xl border border-stone-200 bg-stone-950 p-4 text-xs text-stone-100">
                        {selectedRow ? JSON.stringify(selectedRow, null, 2) : "No row selected."}
                      </pre>
                    </CardContent>
                  </Card>
                </div>
              </TabsContent>
            ))}
          </Tabs>
        </CardContent>
      </Card>
    </div>
  );
}

function TasksView({ tasks, selectedTask, selectedTaskId, onSelectTask }) {
  return (
    <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
      <Card>
        <CardHeader>
          <CardTitle>Tasks</CardTitle>
          <CardDescription>运行列表、中断恢复状态和最近心跳。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {tasks.length ? (
              tasks.map((task) => (
                <button
                  key={task.task_id}
                  className={`w-full rounded-2xl border p-4 text-left transition ${
                    task.task_id === selectedTaskId ? "border-teal-700 bg-teal-50" : "border-stone-200 bg-stone-50 hover:bg-white"
                  }`}
                  onClick={() => onSelectTask(task.task_id)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="truncate font-medium">{task.task_id}</div>
                    <HealthBadge health={task.monitor?.health} />
                  </div>
                  <div className="mt-2 text-sm text-stone-500">{task.current_step || "-"}</div>
                  <div className="mt-1 text-xs text-stone-400">{task.last_heartbeat || "-"}</div>
                </button>
              ))
            ) : (
              <div className="text-sm text-stone-500">No tasks.</div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Task Detail</CardTitle>
          <CardDescription>查看恢复提示、关键指标和最近步骤。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedTask ? (
            <div className="space-y-5">
              <div className="flex flex-wrap items-center gap-2">
                <HealthBadge health={selectedTask.monitor?.health} />
                <Badge variant="outline">{selectedTask.status || "unknown"}</Badge>
                <Badge variant="warm">{selectedTask.current_agent || "-"}</Badge>
              </div>
              <p className="text-sm text-stone-500">{selectedTask.resume_hint || "-"}</p>
              <div className="grid gap-3 md:grid-cols-2">
                <Metric label="Current Step" value={selectedTask.current_step || "-"} />
                <Metric label="Heartbeat Age" value={formatAge(selectedTask.monitor?.heartbeat_age_seconds)} />
                <Metric label="Fallbacks" value={selectedTask.monitor?.recent_fallbacks ?? 0} />
                <Metric label="Failures" value={selectedTask.monitor?.recent_failures ?? 0} />
              </div>
              <div className="space-y-3">
                {(selectedTask.recentSteps || []).map((step, index) => (
                  <div key={`${step.timestamp}-${index}`} className="rounded-2xl border border-stone-200 bg-stone-50 p-4">
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-medium">{step.action || "step"}</div>
                      <Badge>{step.status || "unknown"}</Badge>
                    </div>
                    <div className="mt-2 text-xs text-stone-500">{step.timestamp || "-"}</div>
                    {step.error ? <div className="mt-2 text-xs text-rose-700">{step.error}</div> : null}
                    {step.output ? <div className="mt-2 break-all text-xs text-stone-500">{step.output}</div> : null}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="text-sm text-stone-500">No task selected.</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function AgentsView({ trace, selectedAgent, selectedAgentIndex, onSelectAgent }) {
  return (
    <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
      <Card>
        <CardHeader>
          <CardTitle>Agent Timeline</CardTitle>
          <CardDescription>从 `agent_trace.json` 渲染的执行事件序列。</CardDescription>
        </CardHeader>
        <CardContent>
          <ScrollArea className="h-[640px] pr-4">
            <div className="space-y-3">
              {trace.length ? (
                trace.map((entry, index) => (
                  <button
                    key={`${entry.timestamp}-${index}`}
                    className={`w-full rounded-2xl border p-4 text-left transition ${
                      index === selectedAgentIndex ? "border-teal-700 bg-teal-50" : "border-stone-200 bg-stone-50 hover:bg-white"
                    }`}
                    onClick={() => onSelectAgent(index)}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-medium text-amber-900">{entry.agent || entry.event || "event"}</div>
                      <Badge>{entry.event || "trace"}</Badge>
                    </div>
                    <div className="mt-2 text-xs text-stone-500">{entry.timestamp || "-"}</div>
                  </button>
                ))
              ) : (
                <div className="text-sm text-stone-500">No agent trace.</div>
              )}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Agent Detail</CardTitle>
          <CardDescription>查看单条事件的完整上下文。</CardDescription>
        </CardHeader>
        <CardContent>
          <pre className="h-[640px] overflow-auto rounded-2xl border border-stone-200 bg-stone-950 p-4 text-xs text-stone-100">
            {selectedAgent ? JSON.stringify(selectedAgent, null, 2) : "No agent event selected."}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}

function Metric({ label, value }) {
  return (
    <div className="rounded-2xl border border-stone-200 bg-stone-50 p-4">
      <div className="text-xs uppercase tracking-[0.14em] text-stone-500">{label}</div>
      <div className="mt-2 text-lg font-semibold text-amber-900">{String(value)}</div>
    </div>
  );
}

function HealthBadge({ health = "unknown" }) {
  const variant = ["stale", "failed", "blocked", "degraded"].includes(health) ? "danger" : "default";
  return <Badge variant={variant}>{health}</Badge>;
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
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

function formatAge(seconds) {
  if (seconds == null) return "-";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
}
