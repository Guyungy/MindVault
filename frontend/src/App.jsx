import { useEffect, useMemo, useState } from "react";
import { Activity, Database, FileUp, GitBranch, Search, Sparkles } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";

const views = [
  { id: "overview", label: "总览", icon: Sparkles },
  { id: "input", label: "输入", icon: FileUp },
  { id: "tables", label: "数据表", icon: Database },
  { id: "tasks", label: "任务", icon: Activity },
  { id: "agents", label: "智能体", icon: GitBranch },
];

const AGENT_GROUPS = [
  {
    id: "modeling",
    label: "建库组",
    description: "决定知识结构、数据表规划和最终多表输出。",
    agents: ["ontology_agent", "database_builder_agent"],
  },
  {
    id: "parsing",
    label: "解析组",
    description: "负责抽取、关系识别、去重与 schema 初步组织。",
    agents: ["parse_agent", "relation_agent", "dedup_agent", "schema_designer_agent", "placeholder_agent"],
  },
  {
    id: "governance",
    label: "治理组",
    description: "负责 claim 解析后的冲突审计和可信治理。",
    agents: ["claim_resolver_agent", "conflict_auditor_agent"],
  },
  {
    id: "publishing",
    label: "输出组",
    description: "负责洞察、报告和 wiki 输出。",
    agents: ["insight_agent", "report_agent", "wiki_builder_agent"],
  },
];

export default function App() {
  const [workspaces, setWorkspaces] = useState([]);
  const [workspaceId, setWorkspaceId] = useState("");
  const [newWorkspaceName, setNewWorkspaceName] = useState("");
  const [workspaceCreateStatus, setWorkspaceCreateStatus] = useState(null);
  const [payload, setPayload] = useState(null);
  const [error, setError] = useState("");
  const [activeView, setActiveView] = useState("overview");
  const [activeTable, setActiveTable] = useState("");
  const [selectedTaskId, setSelectedTaskId] = useState("");
  const [agentList, setAgentList] = useState([]);
  const [selectedAgentGroupId, setSelectedAgentGroupId] = useState("modeling");
  const [selectedAgentName, setSelectedAgentName] = useState("");
  const [agentSpec, setAgentSpec] = useState(null);
  const [agentConfigInput, setAgentConfigInput] = useState("");
  const [agentPromptInput, setAgentPromptInput] = useState("");
  const [agentSaveStatus, setAgentSaveStatus] = useState(null);
  const [showSystemTables, setShowSystemTables] = useState(false);
  const [query, setQuery] = useState("");
  const [textInput, setTextInput] = useState("");
  const [jsonInput, setJsonInput] = useState("");
  const [fileInput, setFileInput] = useState(null);
  const [noteField, setNoteField] = useState("");
  const [ingestStatus, setIngestStatus] = useState(null);
  const [isPollingTask, setIsPollingTask] = useState(false);

  const tables = payload?.tables || payload?.multiDb?.databases || [];
  const businessTables = tables.filter((table) => table.visibility !== "system");
  const systemTables = tables.filter((table) => table.visibility === "system");
  const visibleTables = showSystemTables ? systemTables : businessTables;
  const tasks = payload?.tasks || [];
  const trace = payload?.trace || [];
  const recentSources = payload?.recentSources || [];
  const latestTask = payload?.latestTask || tasks[0] || null;
  const shouldPollTasks = isPollingTask || tasks.some((task) => task.status === "running");
  const activeTableData = visibleTables.find((table) => table.name === activeTable) || visibleTables[0] || null;
  const totalRows = businessTables.reduce((sum, table) => sum + (table.rows || []).length, 0);
  const activeTasks = tasks.filter((task) => task.status === "running").length;
  const completedTasks = tasks.filter((task) => task.status === "completed").length;
  const staleTasks = tasks.filter((task) => ["stale", "blocked", "failed"].includes(task.monitor?.health || task.status)).length;
  const filteredRows = useMemo(() => {
    const rows = activeTableData?.rows || [];
    const normalized = query.trim().toLowerCase();
    if (!normalized) return rows;
    return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(normalized));
  }, [activeTableData, query]);
  const selectedTask = tasks.find((task) => task.task_id === selectedTaskId) || tasks[0] || null;
  const agentGroups = useMemo(
    () =>
      AGENT_GROUPS.map((group) => ({
        ...group,
        items: group.agents.map((agentName) => agentList.find((agent) => agent.name === agentName)).filter(Boolean),
      })).filter((group) => group.items.length),
    [agentList],
  );
  const selectedAgentGroup =
    agentGroups.find((group) => group.id === selectedAgentGroupId) || agentGroups[0] || null;

  useEffect(() => {
    loadWorkspaces();
    loadAgents();
  }, []);

  useEffect(() => {
    if (!workspaceId) return;
    setPayload(null);
    setQuery("");
    setShowSystemTables(false);
    setActiveTable("");
    setSelectedTaskId("");
    setIngestStatus(null);
    loadWorkspace(workspaceId);
  }, [workspaceId]);

  useEffect(() => {
    if (!visibleTables.length) return;
    if (!visibleTables.some((table) => table.name === activeTable)) {
      setActiveTable(visibleTables[0].name);
    }
  }, [visibleTables, activeTable]);

  useEffect(() => {
    if (!shouldPollTasks || !workspaceId) return;
    const timer = window.setInterval(() => {
      loadWorkspace(workspaceId);
    }, 2000);
    return () => window.clearInterval(timer);
  }, [shouldPollTasks, workspaceId]);

  useEffect(() => {
    if (!isPollingTask || !latestTask) return;
    const message = [
      mapTaskStatus(latestTask.status || "running"),
      latestTask.current_step || "-",
      latestTask.resume_hint || "",
    ]
      .filter(Boolean)
      .join(" · ");
    setIngestStatus({ ok: true, loading: latestTask.status === "running", message });
    if (["completed", "failed", "blocked", "paused"].includes(latestTask.status)) {
      setIsPollingTask(false);
    }
  }, [isPollingTask, latestTask]);

  useEffect(() => {
    if (!selectedAgentName) return;
    loadAgentSpec(selectedAgentName);
  }, [selectedAgentName]);

  useEffect(() => {
    if (!selectedAgentGroup) return;
    if (!selectedAgentGroup.items.some((agent) => agent.name === selectedAgentName)) {
      setSelectedAgentName(selectedAgentGroup.items[0]?.name || "");
    }
  }, [selectedAgentGroup, selectedAgentName]);

  async function loadWorkspaces() {
    try {
      setError("");
      const result = await fetchJson("/api/workspaces");
      const available = result.workspaces || [];
      setWorkspaces(available);
      if (available[0]) {
        setWorkspaceId((current) => current || available[0].id);
      } else {
        setError("没有可用的工作空间。");
      }
    } catch (err) {
      setError(err.message);
    }
  }

  async function loadAgents() {
    try {
      const result = await fetchJson("/api/agents");
      const items = result.agents || [];
      setAgentList(items);
      const initialGroup = AGENT_GROUPS.find((group) => group.agents.some((agent) => items.some((item) => item.name === agent)));
      const initialName = initialGroup?.agents.find((agent) => items.some((item) => item.name === agent)) || items[0]?.name || "";
      setSelectedAgentGroupId(initialGroup?.id || "modeling");
      setSelectedAgentName(initialName);
    } catch (err) {
      setError(err.message);
    }
  }

  async function loadAgentSpec(agentName) {
    try {
      setAgentSaveStatus(null);
      const result = await fetchJson(`/api/agents/${encodeURIComponent(agentName)}`);
      setAgentSpec(result);
      setAgentConfigInput(result.configContent || "");
      setAgentPromptInput(result.promptContent || "");
    } catch (err) {
      setError(err.message);
    }
  }

  async function saveAgentSpec() {
    if (!selectedAgentName) return;
    try {
      setAgentSaveStatus({ ok: true, message: "正在保存..." });
      const response = await fetch(`/api/agents/${encodeURIComponent(selectedAgentName)}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          configContent: agentConfigInput,
          promptContent: agentPromptInput,
        }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.error || `Request failed: ${response.status}`);
      }
      setAgentSpec(result.agent || null);
      setAgentSaveStatus({ ok: true, message: result.message || "已保存。" });
      setAgentConfigInput(result.agent?.configContent || "");
      setAgentPromptInput(result.agent?.promptContent || "");
      loadAgents();
    } catch (err) {
      setAgentSaveStatus({ ok: false, message: err.message });
    }
  }

  async function loadWorkspace(id) {
    try {
      setError("");
      const result = await fetchJson(`/api/workspaces/${encodeURIComponent(id)}`);
      setPayload(result);
      const nextTables = result.tables || result.multiDb?.databases || [];
      const nextBusiness = nextTables.find((table) => table.visibility !== "system");
      const nextSystem = nextTables.find((table) => table.visibility === "system");
      setActiveTable(nextBusiness?.name || nextSystem?.name || "");
      setSelectedTaskId(result.latestTask?.task_id || "");
    } catch (err) {
      setError(err.message);
    }
  }

  async function createWorkspace() {
    const value = newWorkspaceName.trim();
    if (!value) {
      setWorkspaceCreateStatus({ ok: false, message: "请输入工作空间名称。" });
      return;
    }
    try {
      setWorkspaceCreateStatus({ ok: true, message: "正在创建..." });
      const response = await fetch("/api/workspaces", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ workspaceId: value }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.error || `Request failed: ${response.status}`);
      }
      setNewWorkspaceName("");
      setWorkspaceCreateStatus({ ok: true, message: result.message || "工作空间已创建。" });
      await loadWorkspaces();
      setWorkspaceId(result.workspace);
      setActiveView("overview");
    } catch (err) {
      setWorkspaceCreateStatus({ ok: false, message: err.message });
    }
  }

  async function submitIngest() {
    if (!workspaceId) return;
    const formData = new FormData();
    if (textInput.trim()) formData.append("text_input", textInput.trim());
    if (jsonInput.trim()) formData.append("json_input", jsonInput.trim());
    if (fileInput) formData.append("upload", fileInput);
    if (noteField.trim()) formData.append("note", noteField.trim());

    if (![...formData.keys()].length) {
      setIngestStatus({ ok: false, message: "请先填写文本、JSON 或选择文件。" });
      return;
    }

    try {
      setIngestStatus({ loading: true, ok: true, message: "请求已发送，等待任务启动..." });
      const response = await fetch(`/api/workspaces/${encodeURIComponent(workspaceId)}/ingest`, {
        method: "POST",
        body: formData,
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data?.error || `Request failed: ${response.status}`);
      }
      setIngestStatus({ ok: true, loading: true, message: data?.message || "已提交后台处理。" });
      setActiveView("input");
      setIsPollingTask(true);
      setTextInput("");
      setJsonInput("");
      setFileInput(null);
      setNoteField("");
      window.setTimeout(() => loadWorkspace(workspaceId), 500);
    } catch (err) {
      setIsPollingTask(false);
      setIngestStatus({ ok: false, message: err.message });
    }
  }

  return (
    <div className="min-h-screen bg-[linear-gradient(180deg,#f8f3ea_0%,#efe7db_100%)] text-stone-900">
      {error ? <div className="bg-rose-900 px-4 py-3 text-sm text-rose-50">{error}</div> : null}
      <div className="grid min-h-screen lg:grid-cols-[280px_1fr]">
        <aside className="bg-white/65 p-6 backdrop-blur-sm">
          <div className="mb-8">
            <div className="text-xs uppercase tracking-[0.32em] text-stone-500">MindVault</div>
            <h1 className="mt-2 text-2xl font-semibold">控制台</h1>
          </div>

          <label className="mb-2 block text-xs uppercase tracking-[0.16em] text-stone-500">Workspace</label>
          <select
            className="mb-6 flex h-10 w-full rounded-xl border border-stone-200 bg-white px-3 py-2 text-sm text-stone-900 shadow-sm outline-none transition-colors focus-visible:ring-2 focus-visible:ring-teal-700"
            value={workspaceId}
            onChange={(event) => setWorkspaceId(event.target.value)}
          >
            {workspaces.map((workspace) => (
              <option key={workspace.id} value={workspace.id}>
                {workspace.id}
              </option>
            ))}
          </select>

          <div className="mb-6 space-y-2 rounded-2xl bg-white/70 p-3 shadow-sm">
            <div className="text-xs uppercase tracking-[0.16em] text-stone-500">新建工作空间</div>
            <Input
              className="h-10 rounded-xl"
              placeholder="例如 project_alpha"
              value={newWorkspaceName}
              onChange={(event) => setNewWorkspaceName(event.target.value)}
            />
            <Button size="sm" onClick={createWorkspace}>
              创建
            </Button>
            {workspaceCreateStatus ? (
              <div className={`text-xs ${workspaceCreateStatus.ok ? "text-teal-700" : "text-rose-700"}`}>
                {workspaceCreateStatus.message}
              </div>
            ) : null}
          </div>

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
              <div className="text-xs uppercase tracking-[0.24em] text-teal-700">工作区</div>
              <h2 className="mt-2 text-3xl font-semibold">{payload?.workspace || "加载中"}</h2>
              <p className="mt-2 max-w-2xl text-sm text-stone-500">
                {payload?.multiDb?.domain || "显示当前工作区的多表数据、任务状态与智能体轨迹。"}
              </p>
            </div>
            <label className="flex w-full max-w-sm items-center gap-3 rounded-2xl bg-white px-4 py-3 shadow-sm">
              <Search className="h-4 w-4 text-stone-400" />
              <Input
                className="h-auto border-0 bg-transparent px-0 py-0 shadow-none focus-visible:ring-0"
                placeholder="搜索当前数据表..."
                value={query}
                onChange={(event) => setQuery(event.target.value)}
              />
            </label>
          </header>

          {activeView === "overview" ? (
            <OverviewView
              workspaceId={payload?.workspace || workspaceId}
              domain={payload?.multiDb?.domain || ""}
              tables={businessTables}
              systemCount={systemTables.length}
              latestTask={latestTask}
              totalRows={totalRows}
              activeTasks={activeTasks}
              completedTasks={completedTasks}
              staleTasks={staleTasks}
              trace={trace}
            />
          ) : null}

          {activeView === "input" ? (
            <IngestPanel
              noteField={noteField}
              ingestStatus={ingestStatus}
              latestTask={latestTask}
              recentSources={recentSources}
              textInput={textInput}
              jsonInput={jsonInput}
              onTextChange={setTextInput}
              onJsonChange={setJsonInput}
              onFileChange={setFileInput}
              onNoteChange={setNoteField}
              onSubmit={submitIngest}
            />
          ) : null}

          {activeView === "tables" ? (
            <TablesView
              key={`${workspaceId}:${showSystemTables ? "system" : "business"}`}
              workspaceId={payload?.workspace || workspaceId}
              tables={visibleTables}
              businessCount={businessTables.length}
              systemCount={systemTables.length}
              showSystemTables={showSystemTables}
              onToggleVisibility={setShowSystemTables}
              activeTable={activeTable}
              onActiveTableChange={setActiveTable}
              filteredRows={filteredRows}
            />
          ) : null}

          {activeView === "tasks" ? (
            <TasksView tasks={tasks} selectedTask={selectedTask} selectedTaskId={selectedTaskId} onSelectTask={setSelectedTaskId} />
          ) : null}

          {activeView === "agents" ? (
            <AgentsView
              agentGroups={agentGroups}
              selectedAgentGroupId={selectedAgentGroupId}
              selectedAgentName={selectedAgentName}
              selectedAgentGroup={selectedAgentGroup}
              agentSpec={agentSpec}
              configInput={agentConfigInput}
              promptInput={agentPromptInput}
              saveStatus={agentSaveStatus}
              onSelectGroup={setSelectedAgentGroupId}
              onSelectAgent={setSelectedAgentName}
              onConfigChange={setAgentConfigInput}
              onPromptChange={setAgentPromptInput}
              onSave={saveAgentSpec}
            />
          ) : null}
        </main>
      </div>
    </div>
  );
}

function TablesView({
  workspaceId,
  tables,
  businessCount,
  systemCount,
  showSystemTables,
  onToggleVisibility,
  activeTable,
  onActiveTableChange,
  filteredRows,
}) {
  const current = tables.find((table) => table.name === activeTable) || tables[0] || null;
  const currentVisibleColumns = getVisibleColumns(current);

  return (
    <Card>
      <CardHeader>
        <CardTitle>数据表</CardTitle>
        <CardDescription>
          当前工作区：{workspaceId || "-"}。切换工作区时，这里会只显示该工作区自己的数据表。
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="mb-4 flex flex-wrap items-center gap-3">
          <Button variant={showSystemTables ? "outline" : "default"} size="sm" onClick={() => onToggleVisibility(false)}>
            业务表
            <Badge variant="outline" className="ml-1">{businessCount}</Badge>
          </Button>
          <Button variant={showSystemTables ? "default" : "outline"} size="sm" onClick={() => onToggleVisibility(true)}>
            系统表
            <Badge variant="outline" className="ml-1">{systemCount}</Badge>
          </Button>
        </div>
        {!tables.length ? <div className="text-sm text-stone-500">当前没有数据表。</div> : null}
        {tables.length ? (
          <Tabs value={current?.name} onValueChange={onActiveTableChange}>
            <TabsList>
              {tables.map((table) => (
                <TabsTrigger key={table.name} value={table.name}>
                  {table.title || table.name}
                </TabsTrigger>
              ))}
            </TabsList>

            {tables.map((table) => (
              <TabsContent key={table.name} value={table.name}>
                <div className="mb-4 flex items-center gap-3">
                  <Badge variant="outline">{(table.rows || []).length} 行</Badge>
                  <Badge variant="outline">{getVisibleColumns(table).length} 列</Badge>
                  {getHiddenMetaColumns(table).length ? (
                    <Badge variant="outline">隐藏 {getHiddenMetaColumns(table).length} 个系统字段</Badge>
                  ) : null}
                </div>
                <ScrollArea className="h-[640px] rounded-2xl bg-white">
                  <table className="w-full border-collapse text-sm">
                    <thead className="sticky top-0 bg-stone-100 text-stone-700">
                      <tr>
                        {getVisibleColumns(table).map((column) => (
                          <th key={column} className="border-b border-stone-200 px-4 py-3 text-left font-medium">
                            {formatColumnLabel(column)}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {filteredRows.map((row, index) => (
                        <tr key={`${table.name}-${index}`} className="odd:bg-white even:bg-stone-50">
                          {getVisibleColumns(table).map((column) => (
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

function OverviewView({
  workspaceId,
  domain,
  tables,
  systemCount,
  latestTask,
  totalRows,
  activeTasks,
  completedTasks,
  staleTasks,
  trace,
}) {
  const topTables = [...tables]
    .sort((left, right) => (right.rows || []).length - (left.rows || []).length)
    .slice(0, 4);

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="业务表" value={tables.length} hint={`系统表 ${systemCount} 张`} />
        <MetricCard label="记录总数" value={totalRows} hint="所有业务表行数汇总" />
        <MetricCard label="运行中任务" value={activeTasks} hint="同一工作区建议串行执行" />
        <MetricCard label="异常任务" value={staleTasks} hint="中断、失败或阻塞的任务" />
      </div>

      <div className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
        <Card>
          <CardHeader>
            <CardTitle>工作区摘要</CardTitle>
            <CardDescription>先看结构，再进入具体数据表或任务页。</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <SummaryRow label="工作区" value={workspaceId || "-"} />
            <SummaryRow label="知识域" value={domain || "未提供"} />
            <SummaryRow label="已完成任务" value={String(completedTasks)} />
            <SummaryRow label="最近任务" value={latestTask?.task_id || "暂无"} />
            <SummaryRow label="最近步骤" value={latestTask?.current_step || "-"} />
            <SummaryRow label="最近提示" value={latestTask?.resume_hint || "暂无"} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>数据表概览</CardTitle>
            <CardDescription>优先展示记录较多的几张表。</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {topTables.length ? (
              topTables.map((table) => (
                <div key={table.name} className="rounded-2xl bg-white px-4 py-4 shadow-sm">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="font-medium text-stone-900">{table.title || table.name}</div>
                      <div className="mt-1 text-xs text-stone-500">{table.name}</div>
                    </div>
                    <Badge variant="outline">{(table.rows || []).length} 行</Badge>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2 text-xs text-stone-500">
                    <span>{(table.columns || []).length} 列</span>
                    <span>·</span>
                    <span>{previewColumns(table.columns)}</span>
                  </div>
                </div>
              ))
            ) : (
              <div className="text-sm text-stone-500">当前还没有数据表结构。</div>
            )}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>最近智能体轨迹</CardTitle>
          <CardDescription>只展示最近 6 条事件，详细内容在“智能体”页查看。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {trace.slice(0, 6).map((entry, index) => (
              <div key={`${entry.timestamp}-${index}`} className="rounded-2xl bg-white px-4 py-4 shadow-sm">
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium text-stone-900">{entry.agent || entry.event || "事件"}</div>
                  <Badge variant="outline">{entry.event || "轨迹"}</Badge>
                </div>
                <div className="mt-2 text-xs text-stone-500">{entry.timestamp || "-"}</div>
              </div>
            ))}
            {!trace.length ? <div className="text-sm text-stone-500">暂无智能体轨迹。</div> : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function IngestPanel({
  noteField,
  ingestStatus,
  latestTask,
  recentSources,
  textInput,
  jsonInput,
  onTextChange,
  onJsonChange,
  onFileChange,
  onNoteChange,
  onSubmit,
}) {
  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>输入</CardTitle>
        <CardDescription>资料输入单独放在这个标签页。支持文本、文件、JSON 列表和备注。归入哪些数据表由 AI 自动判断。</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid gap-4 lg:grid-cols-2">
          <label className="space-y-2 text-sm text-stone-600">
            <span>纯文本</span>
            <Textarea
              className="min-h-28 rounded-2xl"
              placeholder="粘贴原始资料文本"
              value={textInput}
              onChange={(event) => onTextChange(event.target.value)}
            />
          </label>

          <label className="space-y-2 text-sm text-stone-600">
            <span>JSON 列表</span>
            <Textarea
              className="min-h-28 rounded-2xl"
              placeholder='例如: [{"source_id":"s1","content":"..."}]'
              value={jsonInput}
              onChange={(event) => onJsonChange(event.target.value)}
            />
          </label>
        </div>

        <div className="mt-4">
          <label className="space-y-2 text-sm text-stone-600">
            <span>上传文件</span>
            <Input
              type="file"
              className="h-auto rounded-2xl py-3"
              onChange={(event) => onFileChange(event.target.files?.[0] || null)}
            />
          </label>
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-[1fr_auto] lg:items-end">
          <label className="space-y-2 text-sm text-stone-600">
            <span>备注</span>
            <Input
              className="h-12 rounded-2xl"
              value={noteField}
              onChange={(event) => onNoteChange(event.target.value)}
              placeholder="可选备注，会作为 AI 归档判断的补充提示"
            />
          </label>
          <Button onClick={onSubmit} disabled={latestTask?.status === "running"}>
            {latestTask?.status === "running" ? "任务运行中" : "提交资料"}
          </Button>
        </div>

        {ingestStatus ? (
          <div className="mt-4 rounded-2xl bg-stone-50 p-4">
            <div className={`text-sm ${ingestStatus.ok ? "text-teal-700" : ingestStatus.loading ? "text-stone-500" : "text-rose-700"}`}>
              {ingestStatus.message}
            </div>
            {latestTask?.status === "running" ? (
              <div className="mt-2 text-xs text-stone-500">同一个工作区当前只建议串行运行一个任务，避免同时写同一批产物文件。</div>
            ) : null}
            <div className="mt-2 text-xs text-stone-500">系统会根据内容和备注自动判断归入哪些业务表，不再手动选表。</div>
            {latestTask ? (
              <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-stone-500">
                <Badge variant="outline">{latestTask.task_id}</Badge>
                <Badge variant="outline">{mapTaskStatus(latestTask.status || "unknown")}</Badge>
                <span>{latestTask.current_step || "-"}</span>
                <span>{latestTask.resume_hint || ""}</span>
              </div>
            ) : null}
          </div>
        ) : null}

        <div className="mt-6">
          <Separator className="mb-6" />
          <div className="mb-3 flex items-center justify-between gap-3">
            <div>
              <div className="text-sm font-medium text-stone-900">最近写入来源</div>
              <div className="text-xs text-stone-500">提交成功后，新增 source 会先出现在这里，再进入后续数据表与智能体流程。</div>
            </div>
            <Badge variant="outline">{recentSources.length} 条</Badge>
          </div>

          {recentSources.length ? (
            <div className="space-y-3">
              {recentSources.map((source, index) => (
                <div key={`${source.source_id || "source"}-${index}`} className="rounded-2xl bg-white px-4 py-4 shadow-sm">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-medium text-stone-900">{source.source_id || "未命名来源"}</span>
                    <Badge variant="outline">{source.source_type || "doc"}</Badge>
                  </div>
                  <div className="mt-2 text-xs text-stone-500">
                    {source.metadata?.filename || "手动输入"}
                    {source.ingested_at ? ` · ${source.ingested_at}` : ""}
                  </div>
                  {source.context_hints?.note ? <div className="mt-2 text-sm text-stone-600">备注：{source.context_hints.note}</div> : null}
                  <div className="mt-2 line-clamp-3 text-sm text-stone-600">{source.content || "无内容"}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="rounded-2xl bg-white px-4 py-4 text-sm text-stone-500 shadow-sm">当前还没有最近写入记录。</div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function TasksView({ tasks, selectedTask, selectedTaskId, onSelectTask }) {
  const timeline = selectedTask?.stepTimeline || condenseStepEntries(selectedTask?.recentSteps || []);
  const eventLog = selectedTask?.stepEntries || selectedTask?.recentSteps || [];

  return (
    <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
      <Card>
        <CardHeader>
          <CardTitle>任务</CardTitle>
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
                    <StatusBadge task={task} />
                  </div>
                  <div className="mt-2 text-sm text-stone-500">{mapTaskStatus(task.status)} · {task.current_step || "-"}</div>
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
          <CardTitle>任务详情</CardTitle>
          <CardDescription>查看当前步骤、最近事件与产物输出。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedTask ? (
            <div className="space-y-5">
              <div className="grid gap-4 md:grid-cols-2">
                <TaskMetric label="状态" value={mapTaskStatus(selectedTask.status)} task={selectedTask} />
                <TaskMetric label="当前步骤" value={selectedTask.current_step || "-"} />
                <TaskMetric label="当前智能体" value={selectedTask.current_agent || "未指定"} />
                <TaskMetric label="最后心跳" value={formatTaskTime(selectedTask.last_heartbeat)} />
              </div>

              <div className="rounded-2xl bg-stone-50 p-4">
                <div className="text-sm font-medium text-stone-900">恢复提示</div>
                <div className="mt-2 text-sm text-stone-600">{selectedTask.resume_hint || "暂无"}</div>
              </div>

              <div className="rounded-2xl bg-stone-50 p-4">
                <div className="text-sm font-medium text-stone-900">本次更新影响</div>
                {selectedTask.impact?.source_count ? (
                  <div className="mt-3 space-y-3">
                    <div className="flex flex-wrap items-center gap-2 text-sm text-stone-600">
                      <Badge variant="outline">{selectedTask.impact.source_count} 条来源</Badge>
                      {(selectedTask.impact.databases || []).map((database) => (
                        <Badge key={database.name} variant="outline">
                          {database.title} · {database.row_count} 条
                        </Badge>
                      ))}
                    </div>
                    <div className="space-y-2">
                      {(selectedTask.impact.databases || []).length ? (
                        selectedTask.impact.databases.map((database) => (
                          <div key={database.name} className="rounded-2xl bg-white px-4 py-3 shadow-sm">
                            <div className="font-medium text-stone-900">
                              {database.title} · {database.row_count} 条记录
                            </div>
                            <div className="mt-2 text-sm text-stone-600">
                              {(database.sample_rows || []).join(" / ") || "已合并到现有记录"}
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="text-sm text-stone-500">这次来源已进入任务，但还没有映射出明显的新数据表记录。</div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="mt-2 text-sm text-stone-500">暂时还没有统计到本次任务对应的来源变更。</div>
                )}
              </div>

              <Separator />

              <div>
                <div className="mb-3 text-sm font-medium text-stone-900">阶段时间线</div>
                <div className="space-y-3">
                  {timeline.length ? (
                    timeline.map((step, index) => (
                      <div key={`${step.timestamp || index}-${step.action || step.status}`} className="rounded-2xl bg-white px-4 py-4 shadow-sm">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div className="font-medium text-stone-900">{step.action || "-"}</div>
                          <Badge variant={mapStepVariant(step.status)}>{mapStepStatus(step.status)}</Badge>
                        </div>
                        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-stone-500">
                          <span>{step.agent || "系统步骤"}</span>
                          <span>·</span>
                          <span>
                            {step.started_at ? `开始：${formatTaskTime(step.started_at)}` : formatTaskTime(step.timestamp)}
                          </span>
                          {step.completed_at ? (
                            <>
                              <span>·</span>
                              <span>完成：{formatTaskTime(step.completed_at)}</span>
                            </>
                          ) : null}
                          {step.started_at && step.completed_at ? (
                            <>
                              <span>·</span>
                              <span>耗时：{formatDuration(step.started_at, step.completed_at)}</span>
                            </>
                          ) : null}
                        </div>
                        {step.resume_hint ? <div className="mt-2 text-sm text-stone-600">{step.resume_hint}</div> : null}
                        {step.outputs?.length ? (
                          <div className="mt-3 flex flex-wrap gap-2">
                            {step.outputs.map((item) => (
                              <Badge key={item} variant="outline">
                                {item}
                              </Badge>
                            ))}
                          </div>
                        ) : null}
                        {step.chunk_ids?.length ? (
                          <div className="mt-3 text-xs text-stone-500">Chunk：{step.chunk_ids.join(" / ")}</div>
                        ) : null}
                      </div>
                    ))
                  ) : (
                    <div className="rounded-2xl bg-white px-4 py-4 text-sm text-stone-500 shadow-sm">暂无阶段时间线。</div>
                  )}
                </div>
              </div>

              <Separator />

              <div>
                <div className="mb-3 text-sm font-medium text-stone-900">原始事件流</div>
                <ScrollArea className="h-[320px] rounded-2xl bg-white">
                  <div className="space-y-3 p-4">
                    {eventLog.length ? (
                      eventLog.map((step, index) => (
                        <div key={`${step.timestamp || index}-${step.action || step.status}-raw`} className="border-b border-stone-100 pb-3 last:border-b-0">
                          <div className="flex flex-wrap items-center justify-between gap-3">
                            <div className="font-medium text-stone-900">
                              {step.action || "-"} · {mapStepStatus(step.status)}
                            </div>
                            <div className="text-xs text-stone-500">{formatTaskTime(step.timestamp)}</div>
                          </div>
                          <div className="mt-1 text-xs text-stone-500">{step.agent || "系统步骤"}</div>
                          {step.resume_hint ? <div className="mt-2 text-sm text-stone-600">{step.resume_hint}</div> : null}
                          {Object.keys(step).length ? (
                            <pre className="mt-2 overflow-auto rounded-xl bg-stone-50 p-3 text-xs text-stone-700">
                              {JSON.stringify(step, null, 2)}
                            </pre>
                          ) : null}
                        </div>
                      ))
                    ) : (
                      <div className="text-sm text-stone-500">暂无事件日志。</div>
                    )}
                  </div>
                </ScrollArea>
              </div>

              <Separator />

              <div>
                <div className="mb-3 text-sm font-medium text-stone-900">产物</div>
                <div className="space-y-2">
                  {Object.entries(selectedTask.artifacts || {}).length ? (
                    Object.entries(selectedTask.artifacts || {}).map(([key, value]) => (
                      <div key={key} className="rounded-2xl bg-white px-4 py-3 shadow-sm">
                        <div className="text-xs uppercase tracking-[0.16em] text-stone-500">{key}</div>
                        <div className="mt-1 break-all text-sm text-stone-700">{String(value)}</div>
                      </div>
                    ))
                  ) : (
                    <div className="rounded-2xl bg-white px-4 py-4 text-sm text-stone-500 shadow-sm">暂无产物记录。</div>
                  )}
                </div>
              </div>

              <details className="rounded-2xl bg-stone-950 p-4 text-stone-100">
                <summary className="cursor-pointer text-sm font-medium">查看完整 JSON</summary>
                <pre className="mt-4 overflow-auto text-xs">{JSON.stringify(selectedTask, null, 2)}</pre>
              </details>
            </div>
          ) : (
            <div className="text-sm text-stone-500">未选择任务。</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function AgentsView({
  agentGroups,
  selectedAgentGroupId,
  selectedAgentName,
  selectedAgentGroup,
  agentSpec,
  configInput,
  promptInput,
  saveStatus,
  onSelectGroup,
  onSelectAgent,
  onConfigChange,
  onPromptChange,
  onSave,
}) {
  return (
    <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
      <Card>
        <CardHeader>
          <CardTitle>智能体</CardTitle>
          <CardDescription>默认只管理 4 个功能组。内部 agent 收到高级设置里，避免界面过度暴露实现细节。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {agentGroups.length ? (
              agentGroups.map((group) => (
                <button
                  key={group.id}
                  className={`w-full rounded-2xl bg-white px-4 py-4 text-left shadow-sm transition ${
                    group.id === selectedAgentGroupId ? "ring-2 ring-teal-700" : ""
                  }`}
                  onClick={() => onSelectGroup(group.id)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-medium">{group.label}</div>
                    <Badge variant="outline">{group.items.length} 个</Badge>
                  </div>
                  <div className="mt-2 text-xs text-stone-500">{group.description}</div>
                </button>
              ))
            ) : (
              <div className="text-sm text-stone-500">没有可编辑的智能体分组。</div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>提示词编辑</CardTitle>
          <CardDescription>先改功能组，再在需要时展开高级设置编辑组内 agent。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedAgentGroup ? (
            <div className="space-y-4">
              <div className="rounded-2xl bg-stone-50 p-4">
                <div className="text-sm font-medium text-stone-900">{selectedAgentGroup.label}</div>
                <div className="mt-2 text-sm text-stone-600">{selectedAgentGroup.description}</div>
                <div className="mt-3 text-xs text-stone-500">
                  当前这一组包含 {selectedAgentGroup.items.length} 个内部 agent。默认不必逐个调整，只有需要微调实现时再展开。
                </div>
              </div>

              <details className="rounded-2xl bg-white p-4 shadow-sm">
                <summary className="cursor-pointer text-sm font-medium text-stone-900">高级设置：组内 agent</summary>
                <div className="mt-4">
                  <Tabs value={selectedAgentName} onValueChange={onSelectAgent}>
                    <TabsList>
                      {selectedAgentGroup.items.map((agent) => (
                        <TabsTrigger key={agent.name} value={agent.name}>
                          {agent.name.replace("_agent", "")}
                        </TabsTrigger>
                      ))}
                    </TabsList>
                  </Tabs>
                </div>
              </details>

              {agentSpec ? (
                <>
              <div className="grid gap-4 md:grid-cols-2">
                <div className="rounded-2xl bg-stone-50 p-4">
                  <div className="text-xs uppercase tracking-[0.16em] text-stone-500">配置文件</div>
                  <div className="mt-2 break-all text-sm text-stone-700">{agentSpec.configPath}</div>
                </div>
                <div className="rounded-2xl bg-stone-50 p-4">
                  <div className="text-xs uppercase tracking-[0.16em] text-stone-500">提示词文件</div>
                  <div className="mt-2 break-all text-sm text-stone-700">{agentSpec.promptPath || "未配置 prompt_template"}</div>
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium text-stone-900">Agent YAML</div>
                <Textarea className="min-h-[260px] rounded-2xl font-mono text-xs" value={configInput} onChange={(event) => onConfigChange(event.target.value)} />
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium text-stone-900">Prompt 模板</div>
                <Textarea className="min-h-[260px] rounded-2xl font-mono text-xs" value={promptInput} onChange={(event) => onPromptChange(event.target.value)} />
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <Button onClick={onSave}>保存智能体</Button>
                {saveStatus ? (
                  <div className={`text-sm ${saveStatus.ok ? "text-teal-700" : "text-rose-700"}`}>{saveStatus.message}</div>
                ) : null}
              </div>
                </>
              ) : (
                <div className="text-sm text-stone-500">未载入智能体配置。</div>
              )}
            </div>
          ) : (
            <div className="text-sm text-stone-500">未选择智能体分组。</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function MetricCard({ label, value, hint }) {
  return (
    <Card>
      <CardContent className="space-y-2 p-5">
        <div className="text-xs uppercase tracking-[0.18em] text-stone-500">{label}</div>
        <div className="text-3xl font-semibold text-stone-900">{value}</div>
        <div className="text-sm text-stone-500">{hint}</div>
      </CardContent>
    </Card>
  );
}

function TaskMetric({ label, value, task }) {
  return (
    <div className="rounded-2xl bg-white px-4 py-4 shadow-sm">
      <div className="text-xs uppercase tracking-[0.16em] text-stone-500">{label}</div>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <div className="text-lg font-semibold text-stone-900">{value}</div>
        {task ? <MonitorBadge task={task} /> : null}
      </div>
    </div>
  );
}

function SummaryRow({ label, value }) {
  return (
    <div className="flex items-start justify-between gap-4 rounded-2xl bg-white px-4 py-3 shadow-sm">
      <div className="text-sm text-stone-500">{label}</div>
      <div className="max-w-[60%] text-right text-sm font-medium text-stone-900">{value}</div>
    </div>
  );
}

function StatusBadge({ task }) {
  const status = task?.status || "unknown";
  const variant = ["failed", "blocked"].includes(status) ? "danger" : status === "running" ? "warm" : "default";
  return <Badge variant={variant}>{mapTaskStatus(status)}</Badge>;
}

function MonitorBadge({ task }) {
  const health = task?.monitor?.health;
  if (!health || health === task?.status || health === "healthy" || health === "completed") {
    return null;
  }
  const variant = ["stale", "failed", "blocked", "degraded"].includes(health) ? "danger" : "outline";
  return <Badge variant={variant}>监视：{mapTaskStatus(health)}</Badge>;
}

function mapTaskStatus(status) {
  const mapping = {
    running: "运行中",
    completed: "已完成",
    failed: "失败",
    blocked: "已阻塞",
    stale: "已中断",
    degraded: "异常",
    paused: "已暂停",
    healthy: "正常",
    unknown: "未知",
  };
  return mapping[status] || status;
}

function mapStepStatus(status) {
  const mapping = {
    running: "运行中",
    ok: "完成",
    fallback: "回退",
    failed: "失败",
    blocked: "阻塞",
  };
  return mapping[status] || status || "未知";
}

function mapStepVariant(status) {
  if (["failed", "blocked", "stale"].includes(status)) return "danger";
  if (status === "fallback") return "warm";
  return "outline";
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

function previewColumns(columns = []) {
  const visibleColumns = getVisibleColumns({ columns });
  if (!visibleColumns.length) return "暂无字段";
  const selected = visibleColumns.slice(0, 4).map((column) => formatColumnLabel(column)).join(" / ");
  const hiddenCount = columns.length - visibleColumns.length;
  if (hiddenCount > 0) {
    return `${selected}${visibleColumns.length > 4 ? " ..." : ""} · 隐藏 ${hiddenCount} 个系统字段`;
  }
  return visibleColumns.length > 4 ? `${selected} ...` : selected;
}

const META_COLUMNS = new Set([
  "id",
  "type",
  "confidence",
  "source_ref",
  "source_refs",
  "updated_at",
  "created_at",
  "tags",
  "canonical_id",
  "claim_type",
]);

function getVisibleColumns(table) {
  const columns = table?.columns || [];
  if (table?.visibility === "system") return columns;
  const filtered = columns.filter((column) => !META_COLUMNS.has(column));
  return filtered.length ? filtered : columns;
}

function getHiddenMetaColumns(table) {
  const columns = table?.columns || [];
  if (table?.visibility === "system") return [];
  return columns.filter((column) => META_COLUMNS.has(column));
}

function formatColumnLabel(column) {
  const labels = {
    name: "名称",
    title: "标题",
    description: "说明",
    location: "地点",
    area: "区域",
    city: "城市",
    address: "地址",
    schedule: "时间",
    opening_hours: "开放时间",
    status: "状态",
    note: "备注",
    notes: "备注",
    phone: "电话",
    website: "链接",
  };
  return labels[column] || column;
}

function formatTaskTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatDuration(startValue, endValue) {
  const start = Date.parse(startValue || "");
  const end = Date.parse(endValue || "");
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return "-";
  const totalSeconds = Math.floor((end - start) / 1000);
  if (totalSeconds < 60) return `${totalSeconds} 秒`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes} 分 ${seconds} 秒`;
}

function condenseStepEntries(steps) {
  const condensed = [];
  for (const step of steps) {
    const last = condensed[condensed.length - 1];
    if (last && last.action === step.action) {
      if (step.status === "running" && !last.started_at) {
        last.started_at = step.timestamp;
        if (!last.resume_hint && step.resume_hint) last.resume_hint = step.resume_hint;
        continue;
      }
      if (last.status === "running" && step.status !== "running") {
        last.status = step.status;
        last.completed_at = step.timestamp;
        last.timestamp = step.timestamp;
        if (!last.resume_hint && step.resume_hint) last.resume_hint = step.resume_hint;
        continue;
      }
    }

    condensed.push({
      ...step,
      started_at: step.status === "running" ? step.timestamp : undefined,
      completed_at: step.status !== "running" ? step.timestamp : undefined,
    });
  }
  return condensed;
}
