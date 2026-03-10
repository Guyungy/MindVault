import { useEffect, useMemo, useState } from "react";
import { Activity, Database, GitBranch, Search } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
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
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [selectedAgentIndex, setSelectedAgentIndex] = useState(0);
  const [query, setQuery] = useState("");

  const databases = payload?.multiDb?.databases || [];
  const tasks = payload?.tasks || [];
  const trace = payload?.trace || [];
  const activeDb = databases.find((db) => db.name === activeDatabase) || databases[0] || null;
  const filteredRows = useMemo(() => {
    const rows = activeDb?.rows || [];
    const normalized = query.trim().toLowerCase();
    if (!normalized) return rows;
    return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(normalized));
  }, [activeDb, query]);
  const selectedTask = tasks.find((task) => task.task_id === selectedTaskId) || tasks[0] || null;
  const selectedAgent = trace[selectedAgentIndex] || trace[0] || null;

  useEffect(() => {
    loadWorkspaces();
  }, []);

  useEffect(() => {
    if (!workspaceId) return;
    loadWorkspace(workspaceId);
  }, [workspaceId]);

  async function loadWorkspaces() {
    try {
      setError("");
      const result = await fetchJson("/api/workspaces");
      const available = (result.workspaces || []).filter((item) => item.hasMultiDb);
      setWorkspaces(available);
      if (available[0]) {
        setWorkspaceId(available[0].id);
      } else {
        setError("没有可用的 multi_db 工作区。");
      }
    } catch (err) {
      setError(err.message);
    }
  }

  async function loadWorkspace(id) {
    try {
      setError("");
      const result = await fetchJson(`/api/workspaces/${encodeURIComponent(id)}`);
      setPayload(result);
      setActiveDatabase(result.multiDb?.databases?.[0]?.name || "");
      setSelectedTaskId(result.latestTask?.task_id || "");
      setSelectedAgentIndex(0);
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div className="min-h-screen bg-[linear-gradient(180deg,#f8f3ea_0%,#efe7db_100%)] text-stone-900">
      {error ? <div className="bg-rose-900 px-4 py-3 text-sm text-rose-50">{error}</div> : null}
      <div className="grid min-h-screen lg:grid-cols-[280px_1fr]">
        <aside className="bg-white/65 p-6 backdrop-blur-sm">
          <div className="mb-8">
            <div className="text-xs uppercase tracking-[0.32em] text-stone-500">MindVault</div>
            <h1 className="mt-2 text-2xl font-semibold">Console</h1>
          </div>

          <label className="mb-2 block text-xs uppercase tracking-[0.16em] text-stone-500">Workspace</label>
          <select
            className="mb-6 w-full rounded-2xl border border-stone-200 bg-white px-3 py-2 text-sm outline-none focus:border-teal-700"
            value={workspaceId}
            onChange={(event) => setWorkspaceId(event.target.value)}
          >
            {workspaces.map((workspace) => (
              <option key={workspace.id} value={workspace.id}>
                {workspace.id}
              </option>
            ))}
          </select>

          <div className="space-y-2">
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
          <header className="mb-6 flex flex-col gap-4 rounded-[28px] bg-white/75 p-6 shadow-sm lg:flex-row lg:items-end lg:justify-between">
            <div>
              <div className="text-xs uppercase tracking-[0.24em] text-teal-700">Workspace</div>
              <h2 className="mt-2 text-3xl font-semibold">{payload?.workspace || "Loading"}</h2>
              <p className="mt-2 max-w-2xl text-sm text-stone-500">
                {payload?.multiDb?.domain || "显示当前工作区的多表数据、任务状态与 agent 轨迹。"}
              </p>
            </div>
            <label className="flex w-full max-w-sm items-center gap-3 rounded-2xl bg-white px-4 py-3 shadow-sm">
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
            />
          ) : null}

          {activeView === "tasks" ? (
            <TasksView tasks={tasks} selectedTask={selectedTask} selectedTaskId={selectedTaskId} onSelectTask={setSelectedTaskId} />
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

function DatabasesView({ databases, activeDatabase, onActiveDatabaseChange, filteredRows }) {
  const current = databases.find((db) => db.name === activeDatabase) || databases[0] || null;

  return (
    <Card>
      <CardHeader>
        <CardTitle>Databases</CardTitle>
        <CardDescription>只读浏览模式。先稳定显示数据，编辑和输入后续再接回。</CardDescription>
      </CardHeader>
      <CardContent>
        {!databases.length ? <div className="text-sm text-stone-500">当前没有数据库数据。</div> : null}
        {databases.length ? (
          <Tabs value={current?.name} onValueChange={onActiveDatabaseChange}>
            <TabsList>
              {databases.map((database) => (
                <TabsTrigger key={database.name} value={database.name}>
                  {database.title || database.name}
                </TabsTrigger>
              ))}
            </TabsList>

            {databases.map((database) => (
              <TabsContent key={database.name} value={database.name}>
                <div className="mb-4 flex items-center gap-3">
                  <Badge variant="outline">{(database.rows || []).length} rows</Badge>
                  <Badge variant="outline">{(database.columns || []).length} fields</Badge>
                </div>
                <ScrollArea className="h-[640px] rounded-2xl bg-white">
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
                        <tr key={`${database.name}-${index}`} className="odd:bg-white even:bg-stone-50">
                          {(database.columns || []).map((column) => (
                            <td key={`${column}-${index}`} className="border-b border-stone-100 px-4 py-3 align-top">
                              <div className="max-w-[320px] truncate">{stringifyValue(row[column])}</div>
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </ScrollArea>
              </TabsContent>
            ))}
          </Tabs>
        ) : null}
      </CardContent>
    </Card>
  );
}

function TasksView({ tasks, selectedTask, selectedTaskId, onSelectTask }) {
  return (
    <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
      <Card>
        <CardHeader>
          <CardTitle>Tasks</CardTitle>
          <CardDescription>显示最近任务与状态。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {tasks.length ? (
              tasks.map((task) => (
                <button
                  key={task.task_id}
                  className={`w-full rounded-2xl bg-white px-4 py-4 text-left shadow-sm transition ${
                    task.task_id === selectedTaskId ? "ring-2 ring-teal-700" : ""
                  }`}
                  onClick={() => onSelectTask(task.task_id)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="truncate font-medium">{task.task_id}</div>
                    <HealthBadge health={task.monitor?.health} />
                  </div>
                  <div className="mt-2 text-sm text-stone-500">{task.current_step || "-"}</div>
                </button>
              ))
            ) : (
              <div className="text-sm text-stone-500">没有任务记录。</div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Task Detail</CardTitle>
          <CardDescription>查看当前选中任务的元数据。</CardDescription>
        </CardHeader>
        <CardContent>
          <pre className="h-[560px] overflow-auto rounded-2xl bg-stone-950 p-4 text-xs text-stone-100">
            {selectedTask ? JSON.stringify(selectedTask, null, 2) : "No task selected."}
          </pre>
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
          <CardTitle>Agents</CardTitle>
          <CardDescription>显示 `agent_trace.json` 的事件序列。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {trace.length ? (
              trace.map((entry, index) => (
                <button
                  key={`${entry.timestamp}-${index}`}
                  className={`w-full rounded-2xl bg-white px-4 py-4 text-left shadow-sm transition ${
                    index === selectedAgentIndex ? "ring-2 ring-teal-700" : ""
                  }`}
                  onClick={() => onSelectAgent(index)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-medium">{entry.agent || entry.event || "event"}</div>
                    <Badge variant="outline">{entry.event || "trace"}</Badge>
                  </div>
                  <div className="mt-2 text-xs text-stone-500">{entry.timestamp || "-"}</div>
                </button>
              ))
            ) : (
              <div className="text-sm text-stone-500">没有 agent 轨迹。</div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Agent Detail</CardTitle>
          <CardDescription>查看当前选中事件的完整 JSON。</CardDescription>
        </CardHeader>
        <CardContent>
          <pre className="h-[560px] overflow-auto rounded-2xl bg-stone-950 p-4 text-xs text-stone-100">
            {selectedAgent ? JSON.stringify(selectedAgent, null, 2) : "No agent event selected."}
          </pre>
        </CardContent>
      </Card>
    </div>
  );
}

function HealthBadge({ health = "unknown" }) {
  const variant = ["stale", "failed", "blocked", "degraded"].includes(health) ? "danger" : "default";
  return <Badge variant={variant}>{health}</Badge>;
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(data?.error || `Request failed: ${response.status}`);
  }
  return data;
}

function stringifyValue(value) {
  if (Array.isArray(value) || (value && typeof value === "object")) {
    return JSON.stringify(value);
  }
  return value ?? "";
}
