import { useEffect, useMemo, useState } from "react";
import { Activity, Database, FileUp, GitBranch, Search, Sparkles } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Select } from "@/components/ui/select";
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

function parseHashRoute() {
  const raw = window.location.hash.replace(/^#/, "");
  const segments = raw.split("/").filter(Boolean);
  if (!segments.length) {
    return { workspaceId: "", view: "overview", tableName: "", taskId: "", agentGroupId: "", agentName: "" };
  }
  if (segments[0] === "workspace") {
    const view = segments[2] || "overview";
    return {
      workspaceId: decodeURIComponent(segments[1] || ""),
      view,
      tableName: view === "tables" ? decodeURIComponent(segments[3] || "") : "",
      taskId: view === "tasks" ? decodeURIComponent(segments[3] || "") : "",
      agentGroupId: view === "agents" ? decodeURIComponent(segments[3] || "") : "",
      agentName: view === "agents" ? decodeURIComponent(segments[4] || "") : "",
    };
  }
  return { workspaceId: "", view: "overview", tableName: "", taskId: "", agentGroupId: "", agentName: "" };
}

function writeHashRoute(workspaceId, view, options = {}) {
  const { tableName = "", taskId = "", agentGroupId = "", agentName = "" } = options;
  let nextHash = "#/";
  if (workspaceId) {
    nextHash = `#/workspace/${encodeURIComponent(workspaceId)}/${view || "overview"}`;
    if (view === "tables" && tableName) {
      nextHash += `/${encodeURIComponent(tableName)}`;
    }
    if (view === "tasks" && taskId) {
      nextHash += `/${encodeURIComponent(taskId)}`;
    }
    if (view === "agents" && agentGroupId) {
      nextHash += `/${encodeURIComponent(agentGroupId)}`;
      if (agentName) {
        nextHash += `/${encodeURIComponent(agentName)}`;
      }
    }
  }
  if (window.location.hash !== nextHash) {
    window.location.hash = nextHash;
  }
}

const AGENT_GROUPS = [
  {
    id: "modeling",
    label: "建库组",
    description: "决定知识结构、数据表规划和最终多表输出。",
    primaryAgent: "ontology_agent",
    agents: ["ontology_agent", "database_builder_agent"],
  },
  {
    id: "parsing",
    label: "解析组",
    description: "负责抽取、关系识别、去重与 schema 初步组织。",
    primaryAgent: "parse_agent",
    agents: ["parse_agent", "relation_agent", "dedup_agent", "schema_designer_agent", "placeholder_agent"],
  },
  {
    id: "governance",
    label: "治理组",
    description: "负责 claim 解析后的冲突审计和可信治理。",
    primaryAgent: "claim_resolver_agent",
    agents: ["claim_resolver_agent", "conflict_auditor_agent"],
  },
  {
    id: "publishing",
    label: "输出组",
    description: "负责洞察、报告和 wiki 输出。",
    primaryAgent: "report_agent",
    agents: ["insight_agent", "report_agent", "wiki_builder_agent"],
  },
];

const AGENT_LABELS = {
  claim_resolver_agent: "事实解析智能体",
  conflict_auditor_agent: "冲突审计智能体",
  database_builder_agent: "数据表构建智能体",
  dedup_agent: "去重智能体",
  insight_agent: "洞察智能体",
  ontology_agent: "结构规划智能体",
  parse_agent: "解析智能体",
  placeholder_agent: "占位补全智能体",
  relation_agent: "关系识别智能体",
  report_agent: "报告智能体",
  schema_designer_agent: "结构设计智能体",
  wiki_builder_agent: "知识页生成智能体",
  schema_engine: "结构引擎",
  memory_curator: "记忆整理器",
  knowledge_store: "知识存储器",
  governance: "治理引擎",
  version_store: "版本存储器",
  insight_generator: "洞察生成器",
  dashboard_renderer: "控制台渲染器",
  system: "系统步骤",
};

const STEP_LABELS = {
  ingest: "资料接收",
  adapt: "内容切分",
  parse: "知识解析",
  merge: "知识合并",
  governance: "治理审计",
  dashboard: "控制台渲染",
  multi_db: "数据表生成",
  wiki: "知识页生成",
  report: "报告生成",
  insight: "洞察生成",
};

const TRACE_EVENT_LABELS = {
  agent_executed: "智能体执行",
  parse_chunk: "分块解析",
  task_started: "任务开始",
  task_completed: "任务完成",
  task_failed: "任务失败",
};

const CORE_PHASES = [
  { id: "intake", label: "资料接收", actions: ["pipeline_start", "ingest", "adapt"] },
  { id: "parse", label: "知识解析", actions: ["parse"] },
  { id: "curate", label: "知识整理", actions: ["confidence", "schema", "curation", "merge", "governance", "versioning"] },
  { id: "output", label: "内容生成", actions: ["insight", "report", "dashboard"] },
  { id: "tables", label: "数据表生成", actions: ["multi_db", "wiki", "pipeline"] },
];

export default function App() {
  const initialRoute = typeof window !== "undefined" ? parseHashRoute() : { workspaceId: "", view: "overview" };
  const [workspaces, setWorkspaces] = useState([]);
  const [workspaceId, setWorkspaceId] = useState(initialRoute.workspaceId || "");
  const [newWorkspaceName, setNewWorkspaceName] = useState("");
  const [workspaceCreateStatus, setWorkspaceCreateStatus] = useState(null);
  const [payload, setPayload] = useState(null);
  const [modelConfig, setModelConfig] = useState(null);
  const [selectedProviderId, setSelectedProviderId] = useState("");
  const [modelSaveStatus, setModelSaveStatus] = useState(null);
  const [error, setError] = useState("");
  const [activeView, setActiveView] = useState(initialRoute.view || "overview");
  const [activeTable, setActiveTable] = useState(initialRoute.tableName || "");
  const [selectedTaskId, setSelectedTaskId] = useState(initialRoute.taskId || "");
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
    loadModels();
  }, []);

  useEffect(() => {
    const onHashChange = () => {
      const route = parseHashRoute();
      if (route.workspaceId && route.workspaceId !== workspaceId) {
        setWorkspaceId(route.workspaceId);
      }
      if (route.view && route.view !== activeView && views.some((view) => view.id === route.view)) {
        setActiveView(route.view);
      }
      if (route.view === "tables" && route.tableName !== activeTable) {
        setActiveTable(route.tableName);
      }
      if (route.view === "tasks" && route.taskId !== selectedTaskId) {
        setSelectedTaskId(route.taskId);
      }
      if (route.view === "agents" && route.agentGroupId && route.agentGroupId !== selectedAgentGroupId) {
        setSelectedAgentGroupId(route.agentGroupId);
      }
      if (route.view === "agents" && route.agentName !== selectedAgentName) {
        setSelectedAgentName(route.agentName);
      }
    };
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, [workspaceId, activeView, activeTable, selectedTaskId, selectedAgentGroupId, selectedAgentName]);

  useEffect(() => {
    if (!workspaceId) return;
    setPayload(null);
    setQuery("");
    setShowSystemTables(false);
    setActiveTable((current) => (workspaceId !== initialRoute.workspaceId ? "" : current));
    setSelectedTaskId((current) => (workspaceId !== initialRoute.workspaceId ? "" : current));
    setIngestStatus(null);
    loadWorkspace(workspaceId);
  }, [workspaceId]);

  useEffect(() => {
    if (!workspaceId && workspaces[0]?.id) {
      return;
    }
    writeHashRoute(workspaceId, activeView, {
      tableName: activeView === "tables" ? activeTable : "",
      taskId: activeView === "tasks" ? selectedTaskId : "",
      agentGroupId: activeView === "agents" ? selectedAgentGroupId : "",
      agentName: activeView === "agents" ? selectedAgentName : "",
    });
  }, [workspaceId, activeView, activeTable, selectedTaskId, selectedAgentGroupId, selectedAgentName, workspaces]);

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
    const preferredName =
      (selectedAgentGroup.primaryAgent &&
      selectedAgentGroup.items.some((agent) => agent.name === selectedAgentGroup.primaryAgent)
        ? selectedAgentGroup.primaryAgent
        : selectedAgentGroup.items[0]?.name) || "";
    if (!selectedAgentGroup.items.some((agent) => agent.name === selectedAgentName) || selectedAgentName !== preferredName) {
      setSelectedAgentName(preferredName);
    }
  }, [selectedAgentGroup, selectedAgentName]);

  async function loadWorkspaces() {
    try {
      setError("");
      const result = await fetchJson("/api/workspaces");
      const available = result.workspaces || [];
      setWorkspaces(available);
      if (available[0]) {
        setWorkspaceId((current) => {
          const preferred = current || initialRoute.workspaceId;
          if (preferred && available.some((workspace) => workspace.id === preferred)) {
            return preferred;
          }
          return available[0].id;
        });
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
      const route = parseHashRoute();
      const initialGroup =
        AGENT_GROUPS.find((group) => group.id === route.agentGroupId && group.agents.some((agent) => items.some((item) => item.name === agent))) ||
        AGENT_GROUPS.find((group) => group.agents.some((agent) => items.some((item) => item.name === agent)));
      const initialName =
        (route.agentName && items.some((item) => item.name === route.agentName) ? route.agentName : "") ||
        (initialGroup?.primaryAgent && items.some((item) => item.name === initialGroup.primaryAgent) ? initialGroup.primaryAgent : "") ||
        initialGroup?.agents.find((agent) => items.some((item) => item.name === agent)) ||
        items[0]?.name ||
        "";
      setSelectedAgentGroupId(initialGroup?.id || "modeling");
      setSelectedAgentName(initialName);
    } catch (err) {
      setError(err.message);
    }
  }

  async function loadModels() {
    try {
      const result = await fetchJson("/api/models");
      setModelConfig(result);
      setSelectedProviderId(result.currentProviderId || "");
    } catch (err) {
      setError(err.message);
    }
  }

  async function saveModelSelection() {
    if (!selectedProviderId) return;
    try {
      setModelSaveStatus({ ok: true, message: "正在切换模型..." });
      const response = await fetch("/api/models", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ providerId: selectedProviderId }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.error || `Request failed: ${response.status}`);
      }
      setModelConfig(result);
      setSelectedProviderId(result.currentProviderId || selectedProviderId);
      setModelSaveStatus({ ok: true, message: result.message || "默认模型已切换。" });
    } catch (err) {
      setModelSaveStatus({ ok: false, message: err.message });
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
      setActiveTable((current) => {
        if (current && nextTables.some((table) => table.name === current)) return current;
        return nextBusiness?.name || nextSystem?.name || "";
      });
      setSelectedTaskId((current) => {
        if (current && (result.tasks || []).some((task) => task.task_id === current)) return current;
        return result.latestTask?.task_id || "";
      });
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
    <div className="min-h-screen text-stone-900">
      {error ? <div className="bg-rose-900 px-4 py-3 text-sm text-rose-50">{error}</div> : null}
      <div className="grid min-h-screen lg:grid-cols-[248px_1fr]">
        <aside className="border-r border-stone-200/80 bg-white/55 p-5 backdrop-blur-sm">
          <div className="mb-6">
            <div className="text-xs uppercase tracking-[0.32em] text-stone-500">MindVault</div>
            <h1 className="mt-2 text-xl font-semibold">控制台</h1>
          </div>

          <label className="mb-2 block text-xs uppercase tracking-[0.16em] text-stone-500">工作空间</label>
          <Select
            className="mb-5 bg-white"
            value={workspaceId}
            onChange={(event) => setWorkspaceId(event.target.value)}
          >
            {workspaces.map((workspace) => (
              <option key={workspace.id} value={workspace.id}>
                {workspace.id}
              </option>
            ))}
          </Select>

          <div className="mb-5 space-y-2">
            <div className="text-xs uppercase tracking-[0.16em] text-stone-500">新建工作空间</div>
            <Input
              className="h-10 bg-white"
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

        <main className="p-5 lg:p-6">
          <header className="mb-5">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div>
              <div className="text-xs uppercase tracking-[0.24em] text-teal-700">工作区</div>
              <h2 className="mt-2 text-3xl font-semibold">{payload?.workspace || "加载中"}</h2>
              <p className="mt-2 max-w-2xl text-sm text-stone-500">
                {payload?.multiDb?.domain || "显示当前工作区的多表数据、任务状态与智能体轨迹。"}
              </p>
            </div>
              <div className="grid gap-3 lg:min-w-[640px] lg:grid-cols-[1fr_320px]">
                <div className="flex items-center gap-3 rounded-2xl border border-stone-200/80 bg-white/82 px-4 py-3">
                  <Search className="h-4 w-4 text-stone-400" />
                  <Input
                    className="h-auto border-0 bg-transparent px-0 py-0 shadow-none focus-visible:ring-0"
                    placeholder="搜索当前数据表..."
                    value={query}
                    onChange={(event) => setQuery(event.target.value)}
                  />
                </div>
                <div className="flex items-center gap-3 rounded-2xl border border-stone-200/80 bg-white/82 px-4 py-3">
                  <div className="min-w-0 flex-1">
                    <div className="text-xs uppercase tracking-[0.16em] text-stone-500">默认模型</div>
                    <div className="mt-1 truncate text-sm font-medium text-stone-900">
                      {modelConfig?.currentProvider?.title || "未配置"}
                    </div>
                  </div>
                  <Select
                    className="max-w-[220px] border-0 bg-transparent px-0"
                    value={selectedProviderId}
                    onChange={(event) => setSelectedProviderId(event.target.value)}
                  >
                    {(modelConfig?.providers || []).map((provider) => (
                      <option key={provider.id} value={provider.id}>
                        {provider.title} · {provider.model}
                      </option>
                    ))}
                  </Select>
                  <Button size="sm" onClick={saveModelSelection} disabled={!selectedProviderId || selectedProviderId === modelConfig?.currentProviderId}>
                    切换模型
                  </Button>
                </div>
              </div>
            </div>
            {modelSaveStatus ? (
              <div className={`mt-3 text-sm ${modelSaveStatus.ok ? "text-teal-700" : "text-rose-700"}`}>{modelSaveStatus.message}</div>
            ) : null}
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
  const currentRows = filteredRows.slice(0, 24);
  const heroColumn = getPrimaryColumn(current);

  return (
    <div className="grid gap-4 xl:grid-cols-[260px_1fr]">
      <Card>
        <CardHeader>
          <CardTitle>数据表</CardTitle>
          <CardDescription>当前工作区：{workspaceId || "-"}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="mb-4 flex gap-2">
            <Button variant={showSystemTables ? "outline" : "default"} size="sm" onClick={() => onToggleVisibility(false)}>
              业务表
            </Button>
            <Button variant={showSystemTables ? "default" : "outline"} size="sm" onClick={() => onToggleVisibility(true)}>
              系统表
            </Button>
          </div>
          <div className="space-y-2">
            {tables.map((table) => (
              <button
                key={table.name}
                className={`w-full rounded-xl px-3 py-3 text-left transition ${current?.name === table.name ? "bg-teal-50" : "bg-stone-50 hover:bg-stone-100"}`}
                onClick={() => onActiveTableChange(table.name)}
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium text-stone-900">{table.title || table.name}</div>
                  <Badge variant="outline">{(table.rows || []).length}</Badge>
                </div>
                <div className="mt-1 text-xs text-stone-500">{table.description || previewColumns(table.columns)}</div>
              </button>
            ))}
            {!tables.length ? <div className="text-sm text-stone-500">当前没有数据表。</div> : null}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-3">
          <CardTitle>{current?.title || "未选择数据表"}</CardTitle>
          <CardDescription>{current?.description || "更像内容库，而不是传统数据库表格。"}</CardDescription>
        </CardHeader>
        <CardContent>
          {current ? (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="outline">{(current.rows || []).length} 条记录</Badge>
                <Badge variant="outline">{getVisibleColumns(current).length} 个展示字段</Badge>
                {getHiddenMetaColumns(current).length ? <Badge variant="outline">隐藏 {getHiddenMetaColumns(current).length} 个系统字段</Badge> : null}
              </div>

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {currentRows.map((row, index) => (
                  <article key={`${current.name}-${index}`} className="rounded-xl bg-stone-50 p-4">
                    <div className="text-base font-semibold text-stone-900">{stringifyValue(row[heroColumn] || row.title || row.name || row.id || "未命名")}</div>
                    <div className="mt-3 space-y-2">
                      {getVisibleColumns(current)
                        .filter((column) => column !== heroColumn)
                        .slice(0, 5)
                        .map((column) => (
                          <div key={`${current.name}-${index}-${column}`} className="grid grid-cols-[84px_1fr] gap-3 text-sm">
                            <div className="text-stone-500">{formatColumnLabel(column)}</div>
                            <div className="text-stone-800">{stringifyValue(row[column]) || "—"}</div>
                          </div>
                        ))}
                    </div>
                  </article>
                ))}
                {!currentRows.length ? <div className="text-sm text-stone-500">当前没有可展示的记录。</div> : null}
              </div>

              {(current.rows || []).length > currentRows.length ? (
                <div className="text-sm text-stone-500">当前只展示前 {currentRows.length} 条记录，避免内容区过重。</div>
              ) : null}
            </div>
          ) : (
            <div className="text-sm text-stone-500">未选择数据表。</div>
          )}
        </CardContent>
      </Card>
    </div>
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

      <div className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr]">
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
                <div key={table.name} className="rounded-xl bg-stone-50 px-4 py-4">
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
              <div key={`${entry.timestamp}-${index}`} className="rounded-xl bg-stone-50 px-4 py-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium text-stone-900">{formatAgentLabel(entry.agent || "") || formatTraceEventLabel(entry.event)}</div>
                  <Badge variant="outline">{formatTraceEventLabel(entry.event || "轨迹")}</Badge>
                </div>
                <div className="mt-2 text-xs text-stone-500">{entry.timestamp || "-"}</div>
                <div className="mt-1 text-sm text-stone-700">{formatStepLabel(entry.action || entry.step || "")}</div>
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
          <div className="mt-4 rounded-xl bg-stone-50 p-4">
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
                <div key={`${source.source_id || "source"}-${index}`} className="rounded-xl bg-stone-50 px-4 py-4">
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
  const coreTimeline = summarizeCoreTimeline(timeline, selectedTask);
  const eventLog = selectedTask?.stepEntries || selectedTask?.recentSteps || [];

  return (
    <div className="grid gap-4 xl:grid-cols-[260px_1fr]">
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
                  className={`w-full rounded-xl px-4 py-4 text-left transition ${
                    task.task_id === selectedTaskId ? "bg-teal-50" : "bg-stone-50 hover:bg-stone-100"
                  }`}
                  onClick={() => onSelectTask(task.task_id)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="truncate font-medium">{task.task_id}</div>
                    <StatusBadge task={task} />
                  </div>
                  <div className="mt-2 text-sm text-stone-500">
                    {mapTaskStatus(task.status)} · {formatStepLabel(task.current_step || "-")}
                  </div>
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
          <CardDescription>单页查看任务状态、影响、核心流程和产物。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedTask ? (
            <div className="space-y-6">
              <div className="rounded-xl bg-stone-50 p-5">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div>
                    <div className="text-xs uppercase tracking-[0.16em] text-stone-500">任务编号</div>
                    <div className="mt-2 text-xl font-semibold text-stone-900">{selectedTask.task_id}</div>
                    <div className="mt-2 text-sm text-stone-600">{selectedTask.resume_hint || "暂无任务说明"}</div>
                  </div>
                  <StatusBadge task={selectedTask} />
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                <TaskMetric label="状态" value={mapTaskStatus(selectedTask.status)} task={selectedTask} />
                <TaskMetric label="当前步骤" value={formatStepLabel(selectedTask.current_step || "-")} />
                <TaskMetric label="当前智能体" value={formatAgentLabel(selectedTask.current_agent || "未指定")} />
                <TaskMetric label="最后心跳" value={formatTaskTime(selectedTask.last_heartbeat)} />
              </div>

              <div className="rounded-xl bg-stone-50 p-4">
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
                          <div key={database.name} className="rounded-xl bg-stone-50 px-4 py-3">
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

              <div>
                <div className="mb-3 text-sm font-medium text-stone-900">核心流程</div>
                <div className="space-y-3">
                  {coreTimeline.length ? (
                    coreTimeline.map((step, index) => (
                      <div key={`${step.timestamp || index}-${step.action || step.status}`} className="rounded-xl bg-stone-50 px-4 py-4">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div className="font-medium text-stone-900">{step.label || formatStepLabel(step.action || "-")}</div>
                          <Badge variant={mapStepVariant(step.status)}>{mapStepStatus(step.status)}</Badge>
                        </div>
                        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-stone-500">
                          <span>{step.agent_label || formatAgentLabel(step.agent || "系统步骤")}</span>
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
                        {step.actions?.length ? (
                          <div className="mt-3 text-xs text-stone-500">
                            包含步骤：{step.actions.map((name) => formatStepLabel(name)).join(" / ")}
                          </div>
                        ) : null}
                      </div>
                    ))
                  ) : (
                    <div className="rounded-xl bg-stone-50 px-4 py-4 text-sm text-stone-500">暂无核心流程记录。</div>
                  )}
                </div>
              </div>

              <div>
                <div className="mb-3 text-sm font-medium text-stone-900">产物</div>
                <div className="grid gap-3 md:grid-cols-2">
                  {Object.entries(selectedTask.artifacts || {}).length ? (
                    Object.entries(selectedTask.artifacts || {}).map(([key, value]) => (
                      <div key={key} className="rounded-xl bg-stone-50 px-4 py-3">
                        <div className="text-xs uppercase tracking-[0.16em] text-stone-500">{key}</div>
                        <div className="mt-1 break-all text-sm text-stone-700">{String(value)}</div>
                      </div>
                    ))
                  ) : (
                    <div className="rounded-xl bg-stone-50 px-4 py-4 text-sm text-stone-500">暂无产物记录。</div>
                  )}
                </div>
              </div>

              <details className="rounded-xl bg-stone-50 p-4">
                <summary className="cursor-pointer text-sm font-medium text-stone-900">高级日志</summary>
                <div className="mt-4">
                  <div className="mb-3 text-sm font-medium text-stone-900">原始事件流</div>
                  <ScrollArea className="h-[320px] rounded-xl bg-white">
                    <div className="space-y-3 p-4">
                      {eventLog.length ? (
                        eventLog.map((step, index) => (
                          <div key={`${step.timestamp || index}-${step.action || step.status}-raw`} className="border-b border-stone-200 pb-3 last:border-b-0">
                            <div className="flex flex-wrap items-center justify-between gap-3">
                              <div className="font-medium text-stone-900">
                                {formatStepLabel(step.action || "-")} · {mapStepStatus(step.status)}
                              </div>
                              <div className="text-xs text-stone-500">{formatTaskTime(step.timestamp)}</div>
                            </div>
                            <div className="mt-1 text-xs text-stone-500">{formatAgentLabel(step.agent || "系统步骤")}</div>
                            {step.resume_hint ? <div className="mt-2 text-sm text-stone-600">{step.resume_hint}</div> : null}
                            {Object.keys(step).length ? (
                              <pre className="mt-2 overflow-auto rounded-lg bg-stone-50 p-3 text-xs text-stone-700">
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
                <details className="mt-4 rounded-xl bg-stone-950 p-4 text-stone-100">
                  <summary className="cursor-pointer text-sm font-medium">查看完整 JSON</summary>
                  <pre className="mt-4 overflow-auto text-xs">{JSON.stringify(selectedTask, null, 2)}</pre>
                </details>
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
    <div className="grid gap-4 xl:grid-cols-[260px_1fr]">
      <Card>
        <CardHeader>
          <CardTitle>功能组</CardTitle>
          <CardDescription>这里只切换策略分组，不直接暴露底层流程。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            {agentGroups.length ? (
              agentGroups.map((group) => (
                <button
                  key={group.id}
                  className={`w-full rounded-xl px-4 py-4 text-left transition ${
                    group.id === selectedAgentGroupId ? "bg-teal-50" : "bg-stone-50 hover:bg-stone-100"
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
          <CardTitle>功能组设置</CardTitle>
          <CardDescription>更像设置页，而不是逐个编辑器。只保留主智能体的运行配置和提示词。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedAgentGroup ? (
            <div className="space-y-4">
              <div className="grid gap-3 md:grid-cols-3">
                <div className="rounded-xl bg-stone-50 p-4">
                  <div className="text-xs uppercase tracking-[0.16em] text-stone-500">功能组</div>
                  <div className="mt-2 text-sm font-semibold text-stone-900">{selectedAgentGroup.label}</div>
                </div>
                <div className="rounded-xl bg-stone-50 p-4">
                  <div className="text-xs uppercase tracking-[0.16em] text-stone-500">主智能体</div>
                  <div className="mt-2 text-sm font-semibold text-stone-900">{formatAgentLabel(selectedAgentName)}</div>
                </div>
                <div className="rounded-xl bg-stone-50 p-4">
                  <div className="text-xs uppercase tracking-[0.16em] text-stone-500">内部步骤数</div>
                  <div className="mt-2 text-sm font-semibold text-stone-900">{selectedAgentGroup.items.length} 个</div>
                </div>
              </div>

              <div className="rounded-xl bg-stone-50 p-4 text-sm text-stone-600">{selectedAgentGroup.description}</div>

              <div className="rounded-xl bg-stone-50 p-4">
                <div className="text-xs uppercase tracking-[0.16em] text-stone-500">内部步骤</div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {selectedAgentGroup.items.map((agent) => (
                    <Badge key={agent.name} variant={agent.name === selectedAgentName ? "default" : "outline"}>
                      {formatAgentLabel(agent.name)}
                    </Badge>
                  ))}
                </div>
              </div>

              {agentSpec ? (
                <>
              <div className="grid gap-4 md:grid-cols-2">
                <div className="rounded-xl bg-stone-50 p-4">
                  <div className="text-xs uppercase tracking-[0.16em] text-stone-500">运行配置文件</div>
                  <div className="mt-2 break-all text-sm text-stone-700">{agentSpec.configPath}</div>
                </div>
                <div className="rounded-xl bg-stone-50 p-4">
                  <div className="text-xs uppercase tracking-[0.16em] text-stone-500">提示词文件</div>
                  <div className="mt-2 break-all text-sm text-stone-700">{agentSpec.promptPath || "未配置 prompt_template"}</div>
                </div>
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium text-stone-900">运行配置</div>
                <Textarea className="min-h-[260px] rounded-2xl font-mono text-xs" value={configInput} onChange={(event) => onConfigChange(event.target.value)} />
              </div>

              <div className="space-y-2">
                <div className="text-sm font-medium text-stone-900">主提示词</div>
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
    <div className="rounded-xl bg-stone-50 px-4 py-4">
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
    <div className="flex items-start justify-between gap-4 rounded-xl bg-stone-50 px-4 py-3">
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

function formatAgentLabel(name) {
  if (!name) return "";
  return AGENT_LABELS[name] || name;
}

function formatStepLabel(name) {
  if (!name) return "-";
  return STEP_LABELS[name] || name;
}

function formatTraceEventLabel(name) {
  if (!name) return "轨迹";
  return TRACE_EVENT_LABELS[name] || name;
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

function getPrimaryColumn(table) {
  const columns = getVisibleColumns(table);
  const preferred = ["title", "name", "subject", "summary", "description"];
  return preferred.find((column) => columns.includes(column)) || columns[0] || "id";
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

function summarizeCoreTimeline(steps, task) {
  const phases = CORE_PHASES.map((phase) => ({ ...phase }));
  return phases
    .map((phase) => {
      const items = (steps || []).filter((step) => phase.actions.includes(step.action));
      if (!items.length) return null;
      const first = items[0];
      const last = items[items.length - 1];
      const outputs = Array.from(new Set(items.flatMap((item) => item.outputs || [])));
      const chunkIds = Array.from(new Set(items.flatMap((item) => item.chunk_ids || [])));
      const phaseStatus = deriveCorePhaseStatus(items, phase, task);
      return {
        action: phase.id,
        label: phase.label,
        status: phaseStatus,
        started_at: first.started_at || first.timestamp,
        completed_at: phaseStatus === "running" ? undefined : last.completed_at || last.timestamp,
        timestamp: last.timestamp || first.timestamp,
        agent: last.agent || first.agent,
        agent_label: summarizePhaseAgents(items),
        resume_hint: last.resume_hint || first.resume_hint,
        outputs,
        chunk_ids: chunkIds,
        actions: items.map((item) => item.action).filter(Boolean),
      };
    })
    .filter(Boolean);
}

function deriveCorePhaseStatus(items, phase, task) {
  if (items.some((item) => item.status === "failed")) return "failed";
  if (phase.id === "tables" && task?.status === "failed") return "failed";
  if (items.some((item) => item.status === "blocked")) return "blocked";
  if (items.some((item) => item.status === "fallback")) return "fallback";
  if (items.some((item) => item.status === "running")) return "running";
  return "ok";
}

function summarizePhaseAgents(items) {
  const labels = Array.from(
    new Set(
      (items || [])
        .map((item) => formatAgentLabel(item.agent))
        .filter(Boolean)
        .filter((label) => label !== "系统步骤"),
    ),
  );
  return labels.length ? labels.join(" / ") : "系统步骤";
}
