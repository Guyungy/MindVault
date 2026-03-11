import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { Activity, Bot, Circle, Database, FileText, FileUp, GitBranch, Search, Sparkles, Wrench } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { DataTable } from "@/components/ui/data-table";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Select } from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";

const ForceGraph3D = lazy(() => import("react-force-graph-3d"));

const views = [
  { id: "overview", label: "总览", icon: Sparkles },
  { id: "input", label: "输入", icon: FileUp },
  { id: "tables", label: "数据表", icon: Database },
  { id: "analysis", label: "分析", icon: GitBranch },
  { id: "tasks", label: "任务", icon: Activity },
  { id: "settings", label: "设置", icon: Wrench },
  { id: "skills", label: "技能", icon: Wrench },
  { id: "agents", label: "智能体", icon: GitBranch },
];

const SIDEBAR_SECTIONS = [
  { label: "工作台", items: ["overview", "analysis", "tasks"] },
  { label: "知识资产", items: ["tables", "input"] },
  { label: "系统配置", items: ["settings", "skills", "agents"] },
];

function parseHashRoute() {
  const raw = window.location.hash.replace(/^#/, "");
  const segments = raw.split("/").filter(Boolean);
  if (!segments.length) {
    return { workspaceId: "", view: "overview", tableName: "", taskId: "", agentGroupId: "" };
  }
  if (segments[0] === "workspace") {
    const rawView = segments[2] || "overview";
    const view = rawView === "nodes" ? "analysis" : rawView;
    return {
      workspaceId: decodeURIComponent(segments[1] || ""),
      view,
      tableName: view === "tables" ? decodeURIComponent(segments[3] || "") : "",
      taskId: view === "tasks" ? decodeURIComponent(segments[3] || "") : "",
      agentGroupId: view === "agents" ? decodeURIComponent(segments[3] || "") : "",
    };
  }
  return { workspaceId: "", view: "overview", tableName: "", taskId: "", agentGroupId: "" };
}

function writeHashRoute(workspaceId, view, options = {}) {
  const { tableName = "", taskId = "", agentGroupId = "" } = options;
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
    }
  }
  if (window.location.hash !== nextHash) {
    window.location.hash = nextHash;
  }
}

const AGENT_GROUPS = [
  {
    id: "modeling",
    label: "建库智能体",
    description: "决定知识结构、数据表规划和最终多表输出。",
    agents: ["ontology_agent", "database_builder_agent"],
  },
  {
    id: "parsing",
    label: "解析智能体",
    description: "负责抽取、关系识别、去重与 schema 初步组织。",
    agents: ["parse_agent", "relation_agent", "dedup_agent", "schema_designer_agent", "placeholder_agent"],
  },
  {
    id: "governance",
    label: "治理智能体",
    description: "负责 claim 解析后的冲突审计和可信治理。",
    agents: ["claim_resolver_agent", "conflict_auditor_agent"],
  },
  {
    id: "publishing",
    label: "输出智能体",
    description: "负责洞察、报告和 wiki 输出。",
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
  database_plan: "建库规划",
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
  { id: "planning", label: "建库规划", actions: ["database_plan"] },
  { id: "tables", label: "数据表生成", actions: ["multi_db"] },
  { id: "output", label: "内容生成", actions: ["insight", "report", "dashboard"] },
  { id: "wiki", label: "知识页生成", actions: ["wiki"] },
  { id: "pipeline", label: "任务收尾", actions: ["pipeline"] },
];

export default function App() {
  const initialRoute = typeof window !== "undefined" ? parseHashRoute() : { workspaceId: "", view: "overview" };
  const [workspaces, setWorkspaces] = useState([]);
  const [workspaceId, setWorkspaceId] = useState(initialRoute.workspaceId || "");
  const [newWorkspaceName, setNewWorkspaceName] = useState("");
  const [workspaceCreateStatus, setWorkspaceCreateStatus] = useState(null);
  const [workspaceDeleteStatus, setWorkspaceDeleteStatus] = useState(null);
  const [taskDeleteStatus, setTaskDeleteStatus] = useState(null);
  const [payload, setPayload] = useState(null);
  const [modelConfig, setModelConfig] = useState(null);
  const [runtimeSettings, setRuntimeSettings] = useState(null);
  const [selectedProviderId, setSelectedProviderId] = useState("");
  const [modelSaveStatus, setModelSaveStatus] = useState(null);
  const [runtimeSaveStatus, setRuntimeSaveStatus] = useState(null);
  const [selectedExecutionProfile, setSelectedExecutionProfile] = useState("fast");
  const [reportArtifactEnabled, setReportArtifactEnabled] = useState(false);
  const [error, setError] = useState("");
  const [activeView, setActiveView] = useState(initialRoute.view || "overview");
  const [activeTable, setActiveTable] = useState(initialRoute.tableName || "");
  const [selectedTaskId, setSelectedTaskId] = useState(initialRoute.taskId || "");
  const [agentGroupsList, setAgentGroupsList] = useState([]);
  const [skills, setSkills] = useState([]);
  const [selectedSkillId, setSelectedSkillId] = useState("");
  const [skillStatus, setSkillStatus] = useState(null);
  const [selectedAgentGroupId, setSelectedAgentGroupId] = useState("modeling");
  const [agentGroupSpec, setAgentGroupSpec] = useState(null);
  const [agentSoulInput, setAgentSoulInput] = useState("");
  const [agentEnabledSkills, setAgentEnabledSkills] = useState([]);
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
  const agentGroups = useMemo(() => (agentGroupsList.length ? agentGroupsList : AGENT_GROUPS), [agentGroupsList]);
  const selectedAgentGroup = agentGroups.find((group) => group.id === selectedAgentGroupId) || agentGroups[0] || null;

  useEffect(() => {
    loadWorkspaces();
    loadAgentGroups();
    loadSkills();
    loadModels();
    loadRuntimeSettings();
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
    };
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, [workspaceId, activeView, activeTable, selectedTaskId, selectedAgentGroupId]);

  useEffect(() => {
    if (!workspaceId) return;
    setPayload(null);
    setQuery("");
    setShowSystemTables(false);
    setTaskDeleteStatus(null);
    setActiveTable((current) => (workspaceId !== initialRoute.workspaceId ? "" : current));
    setSelectedTaskId((current) => (workspaceId !== initialRoute.workspaceId ? "" : current));
    setIngestStatus(null);
    loadWorkspace(workspaceId);
  }, [workspaceId]);

  useEffect(() => {
    if (activeView !== "tasks") {
      setTaskDeleteStatus(null);
    }
  }, [activeView]);

  useEffect(() => {
    if (!taskDeleteStatus) return undefined;
    const timer = window.setTimeout(() => setTaskDeleteStatus(null), 2500);
    return () => window.clearTimeout(timer);
  }, [taskDeleteStatus]);

  useEffect(() => {
    if (!workspaceId && workspaces[0]?.id) {
      return;
    }
    writeHashRoute(workspaceId, activeView, {
      tableName: activeView === "tables" ? activeTable : "",
      taskId: activeView === "tasks" ? selectedTaskId : "",
      agentGroupId: activeView === "agents" ? selectedAgentGroupId : "",
    });
  }, [workspaceId, activeView, activeTable, selectedTaskId, selectedAgentGroupId, workspaces]);

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
    if (!selectedAgentGroup) return;
    loadAgentGroupSpec(selectedAgentGroup.id);
  }, [selectedAgentGroup]);

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

  async function loadAgentGroups() {
    try {
      const result = await fetchJson("/api/agent-groups");
      const items = result.groups || [];
      setAgentGroupsList(items);
      const route = parseHashRoute();
      const initialGroup = items.find((group) => group.id === route.agentGroupId) || items[0] || null;
      setSelectedAgentGroupId(initialGroup?.id || "modeling");
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

  async function loadRuntimeSettings() {
    try {
      const result = await fetchJson("/api/runtime-settings");
      setRuntimeSettings(result);
      setSelectedExecutionProfile(result.execution?.profile || "fast");
      setReportArtifactEnabled(Boolean(result.artifacts?.report));
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

  async function saveRuntimeSettings() {
    try {
      setRuntimeSaveStatus({ ok: true, message: "正在保存运行设置..." });
      const response = await fetch("/api/runtime-settings", {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          execution: {
            profile: selectedExecutionProfile,
          },
          artifacts: {
            report: reportArtifactEnabled,
          },
        }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.error || `Request failed: ${response.status}`);
      }
      setRuntimeSettings(result);
      setSelectedExecutionProfile(result.execution?.profile || "fast");
      setReportArtifactEnabled(Boolean(result.artifacts?.report));
      setRuntimeSaveStatus({ ok: true, message: result.message || "运行设置已保存。" });
    } catch (err) {
      setRuntimeSaveStatus({ ok: false, message: err.message });
    }
  }

  async function loadSkills() {
    try {
      const result = await fetchJson("/api/skills");
      const items = result.skills || [];
      setSkills(items);
      setSelectedSkillId((current) => current || items[0]?.id || "");
    } catch (err) {
      setError(err.message);
    }
  }

  async function toggleSkillAutoUpdate(skillId, nextValue) {
    try {
      setSkillStatus({ ok: true, message: "正在保存技能设置..." });
      const response = await fetch(`/api/skills/${encodeURIComponent(skillId)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ auto_update: nextValue }),
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result?.error || `Request failed: ${response.status}`);
      setSkillStatus({ ok: true, message: nextValue ? "已开启自动更新。" : "已关闭自动更新。" });
      loadSkills();
    } catch (err) {
      setSkillStatus({ ok: false, message: err.message });
    }
  }

  async function syncSkill(skillId) {
    try {
      setSkillStatus({ ok: true, message: "正在同步技能..." });
      const response = await fetch(`/api/skills/${encodeURIComponent(skillId)}/sync`, { method: "POST" });
      const result = await response.json();
      if (!response.ok) throw new Error(result?.error || `Request failed: ${response.status}`);
      setSkillStatus({ ok: true, message: result.message || "技能已同步。" });
      loadSkills();
    } catch (err) {
      setSkillStatus({ ok: false, message: err.message });
    }
  }

  async function loadAgentGroupSpec(groupId) {
    try {
      setAgentSaveStatus(null);
      const result = await fetchJson(`/api/agent-groups/${encodeURIComponent(groupId)}`);
      setAgentGroupSpec(result);
      setAgentSoulInput(result.soulContent || "");
      setAgentEnabledSkills(result.enabledSkills || []);
    } catch (err) {
      setError(err.message);
    }
  }

  async function saveAgentSpec() {
    if (!selectedAgentGroupId) return;
    try {
      setAgentSaveStatus({ ok: true, message: "正在保存..." });
      const response = await fetch(`/api/agent-groups/${encodeURIComponent(selectedAgentGroupId)}`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          soulContent: agentSoulInput,
          enabledSkills: agentEnabledSkills,
        }),
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.error || `Request failed: ${response.status}`);
      }
      setAgentGroupSpec(result.group || null);
      setAgentSaveStatus({ ok: true, message: result.message || "已保存。" });
      setAgentSoulInput(result.group?.soulContent || "");
      setAgentEnabledSkills(result.group?.enabledSkills || []);
      loadAgentGroups();
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

  async function deleteWorkspace() {
    if (!workspaceId) return;
    const confirmed = window.confirm(`确定删除工作空间「${workspaceId}」吗？该操作会删除该空间下的全部任务和数据。`);
    if (!confirmed) return;
    try {
      setWorkspaceDeleteStatus({ ok: true, message: "正在删除..." });
      const response = await fetch(`/api/workspaces/${encodeURIComponent(workspaceId)}`, {
        method: "DELETE",
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.error || `Request failed: ${response.status}`);
      }
      setWorkspaceDeleteStatus({ ok: true, message: result.message || "工作空间已删除。" });
      setPayload(null);
      setWorkspaceId("");
      await loadWorkspaces();
      setActiveView("overview");
    } catch (err) {
      setWorkspaceDeleteStatus({ ok: false, message: err.message });
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

  async function deleteTask(taskId) {
    if (!workspaceId || !taskId) return;
    const confirmed = window.confirm(`确定删除任务「${taskId}」吗？该任务详情和步骤日志会一并删除。`);
    if (!confirmed) return;
    try {
      setTaskDeleteStatus({ ok: true, message: "正在删除任务...", taskId });
      const response = await fetch(`/api/workspaces/${encodeURIComponent(workspaceId)}/tasks/${encodeURIComponent(taskId)}`, {
        method: "DELETE",
      });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.error || `Request failed: ${response.status}`);
      }
      setTaskDeleteStatus({ ok: true, message: result.message || "任务已删除。", taskId });
      await loadWorkspace(workspaceId);
      setSelectedTaskId((current) => (current === taskId ? "" : current));
    } catch (err) {
      setTaskDeleteStatus({ ok: false, message: err.message, taskId });
    }
  }

  const currentViewMeta = views.find((view) => view.id === activeView) || views[0];

  return (
    <div className="min-h-screen bg-[var(--background)] text-[var(--foreground)]">
      {error ? (
        <div className="mb-4 rounded-[calc(var(--radius)+0.15rem)] bg-[var(--destructive)] px-4 py-3 text-sm text-[var(--primary-foreground)]">
          {error}
        </div>
      ) : null}

      <div className="grid min-h-screen lg:grid-cols-[248px_1fr]">
          <aside className="border-r border-[var(--border)]/80 bg-[var(--sidebar)] px-3 py-4 text-[var(--sidebar-foreground)]">
            <div className="mb-6 flex items-center gap-2.5 px-2">
              <div className="flex h-8 w-8 items-center justify-center rounded-[0.35rem] bg-[var(--background)]">
                <Circle className="h-4 w-4" />
              </div>
              <div>
                <div className="text-base font-semibold">MindVault</div>
                <div className="text-xs text-[var(--muted-foreground)]">知识工作台</div>
              </div>
            </div>

            <div className="mb-5 space-y-2 border-b border-[var(--border)]/70 px-2 pb-4">
              <label className="block text-[11px] uppercase tracking-[0.12em] text-[var(--muted-foreground)]">工作空间</label>
              <Select value={workspaceId} onChange={(event) => setWorkspaceId(event.target.value)}>
                {workspaces.map((workspace) => (
                  <option key={workspace.id} value={workspace.id}>
                    {workspace.id}
                  </option>
                ))}
              </Select>
            </div>

            <div className="space-y-6">
              {SIDEBAR_SECTIONS.map((section) => (
                <div key={section.label} className="space-y-2">
                  <div className="px-2 text-[11px] font-medium text-[var(--muted-foreground)] whitespace-nowrap">{section.label}</div>
                  <div className="space-y-1">
                    {section.items.map((viewId) => {
                      const view = views.find((item) => item.id === viewId);
                      if (!view) return null;
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
                </div>
              ))}
            </div>

            <div className="mt-6 space-y-2 border-t border-[var(--border)]/70 pt-4">
              <div className="flex items-center gap-2 text-xs font-medium">
                <Bot className="h-4 w-4" />
                空间管理
              </div>
              <Input
                className="h-10"
                placeholder="例如 project_alpha"
                value={newWorkspaceName}
                onChange={(event) => setNewWorkspaceName(event.target.value)}
              />
              <div className="flex gap-2">
                <Button size="sm" className="flex-1" onClick={createWorkspace}>
                  创建
                </Button>
                <Button size="sm" variant="outline" className="flex-1" onClick={deleteWorkspace} disabled={!workspaceId}>
                  删除
                </Button>
              </div>
              {workspaceCreateStatus ? (
                <div className={`text-xs ${workspaceCreateStatus.ok ? "text-[var(--primary)]" : "text-[var(--destructive)]"}`}>
                  {workspaceCreateStatus.message}
                </div>
              ) : null}
              {workspaceDeleteStatus ? (
                <div className={`text-xs ${workspaceDeleteStatus.ok ? "text-[var(--primary)]" : "text-[var(--destructive)]"}`}>
                  {workspaceDeleteStatus.message}
                </div>
              ) : null}
            </div>
          </aside>

          <main className="flex min-w-0 flex-col">
            <header className="border-b border-[var(--border)]/80 px-5 py-4">
              <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
                <div className="min-w-0">
                  <div className="text-2xl font-semibold tracking-tight whitespace-nowrap">{currentViewMeta.label}</div>
                  <div className="mt-1 truncate text-sm text-[var(--muted-foreground)]">
                    {payload?.workspace || "加载中"} · {payload?.multiDb?.domain || "当前工作区的知识数据与运行状态"}
                  </div>
                </div>

                <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
                  <div className="flex items-center gap-2 rounded-[calc(var(--radius)+0.08rem)] border border-[var(--border)]/70 bg-[var(--background)] px-3 py-2 lg:min-w-[280px]">
                    <Search className="h-4 w-4 text-[var(--muted-foreground)]" />
                    <Input
                      className="h-auto border-0 bg-transparent px-0 py-0 shadow-none focus-visible:ring-0"
                      placeholder="搜索当前数据表..."
                      value={query}
                      onChange={(event) => setQuery(event.target.value)}
                    />
                  </div>

                  <Button className="h-11 px-5 whitespace-nowrap" onClick={() => setActiveView("input")}>
                    <FileUp className="h-4 w-4" />
                    快速录入
                  </Button>
                </div>
              </div>
            </header>

            <div className="flex-1 p-5">
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
              recentSources={recentSources}
              businessCount={businessTables.length}
              systemCount={systemTables.length}
              showSystemTables={showSystemTables}
                  onToggleVisibility={setShowSystemTables}
                  activeTable={activeTable}
                  onActiveTableChange={setActiveTable}
                  filteredRows={filteredRows}
                />
              ) : null}

              {activeView === "analysis" ? (
                <NodeWorkspaceView
                  workspaceId={payload?.workspace || workspaceId}
                  tables={businessTables}
                  recentSources={recentSources}
                />
              ) : null}

              {activeView === "tasks" ? (
                <TasksView
                  tasks={tasks}
                  selectedTask={selectedTask}
                  selectedTaskId={selectedTaskId}
                  onSelectTask={setSelectedTaskId}
                  onDeleteTask={deleteTask}
                  deleteStatus={taskDeleteStatus}
                />
              ) : null}

              {activeView === "agents" ? (
                <AgentsView
                  agentGroups={agentGroups}
                  skills={skills}
                  selectedAgentGroupId={selectedAgentGroupId}
                  selectedAgentGroup={selectedAgentGroup}
                  agentGroupSpec={agentGroupSpec}
                  soulInput={agentSoulInput}
                  enabledSkills={agentEnabledSkills}
                  saveStatus={agentSaveStatus}
                  onSelectGroup={setSelectedAgentGroupId}
                  onSoulChange={setAgentSoulInput}
                  onToggleSkill={(skillId) =>
                    setAgentEnabledSkills((current) =>
                      current.includes(skillId) ? current.filter((item) => item !== skillId) : [...current, skillId],
                    )
                  }
                  onSave={saveAgentSpec}
                />
              ) : null}

              {activeView === "skills" ? (
                <SkillsView
                  skills={skills}
                  selectedSkillId={selectedSkillId}
                  onSelectSkill={setSelectedSkillId}
                  onToggleAutoUpdate={toggleSkillAutoUpdate}
                  onSyncSkill={syncSkill}
                  status={skillStatus}
                />
              ) : null}

              {activeView === "settings" ? (
                <SettingsView
                  workspaceId={payload?.workspace || workspaceId}
                  modelConfig={modelConfig}
                  runtimeSettings={runtimeSettings}
                  selectedProviderId={selectedProviderId}
                  selectedExecutionProfile={selectedExecutionProfile}
                  reportArtifactEnabled={reportArtifactEnabled}
                  onProviderChange={setSelectedProviderId}
                  onExecutionProfileChange={setSelectedExecutionProfile}
                  onReportArtifactToggle={setReportArtifactEnabled}
                  onSaveModel={saveModelSelection}
                  onSaveRuntimeSettings={saveRuntimeSettings}
                  saveStatus={modelSaveStatus}
                  runtimeSaveStatus={runtimeSaveStatus}
                />
              ) : null}
            </div>
          </main>
        </div>
    </div>
  );
}

function TablesView({
  workspaceId,
  tables,
  recentSources,
  businessCount,
  systemCount,
  showSystemTables,
  onToggleVisibility,
  activeTable,
  onActiveTableChange,
  filteredRows,
}) {
  const [selectedRowIndex, setSelectedRowIndex] = useState(0);
  const [pendingTargetId, setPendingTargetId] = useState("");
  const [viewMode, setViewMode] = useState("table");
  const current = tables.find((table) => table.name === activeTable) || tables[0] || null;
  const currentRows = filteredRows;
  const heroColumn = getPrimaryColumn(current);
  const referenceMap = buildReferenceMap(tables);
  const selectedRow = currentRows[selectedRowIndex] || currentRows[0] || null;
  const selectedConnections = selectedRow ? buildRowConnections(selectedRow, current, tables, referenceMap) : [];
  const selectedSources = selectedRow ? buildRowSourceSnippets(selectedRow, recentSources) : [];
  const visibleColumns = getVisibleColumns(current);
  const rowIdentity = selectedRow?.id || selectedRow?.entity_id || selectedRow?.event_id || selectedRow?.claim_id || selectedRow?.relation_id;

  useEffect(() => {
    setSelectedRowIndex(0);
  }, [activeTable, filteredRows, showSystemTables]);

  useEffect(() => {
    if (!pendingTargetId || !currentRows.length) return;
    const nextIndex = currentRows.findIndex((row) =>
      [row.id, row.entity_id, row.event_id, row.claim_id, row.relation_id].includes(pendingTargetId),
    );
    setSelectedRowIndex(nextIndex >= 0 ? nextIndex : 0);
    setPendingTargetId("");
  }, [pendingTargetId, currentRows]);

  const tableColumns = (visibleColumns || []).map((column) => ({
    accessorKey: column,
    header: formatColumnLabel(column),
    cell: ({ row }) => {
      const value = row.original?.[column];
      const rendered = formatDisplayValue(value, referenceMap) || "—";
      if (column === heroColumn) {
        return <div className="font-medium">{rendered}</div>;
      }
      return <div className="max-w-[280px] truncate text-[var(--muted-foreground)]">{rendered}</div>;
    },
  }));

  return (
    <div className="grid gap-5 xl:grid-cols-[220px_minmax(0,1fr)]">
      <Card className="border-r border-[var(--border)]/70 bg-transparent">
        <CardHeader>
          <CardTitle>数据表</CardTitle>
          <CardDescription>当前工作区：{workspaceId || "-"}</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="mb-3 flex gap-2 border-b border-[var(--border)]/60 pb-3">
            <Button variant={showSystemTables ? "outline" : "default"} size="sm" onClick={() => onToggleVisibility(false)}>
              业务表
            </Button>
            <Button variant={showSystemTables ? "default" : "outline"} size="sm" onClick={() => onToggleVisibility(true)}>
              系统表
            </Button>
          </div>
          <div className="space-y-1">
            {tables.map((table) => (
              <button
                key={table.name}
                className={`w-full border-b border-[var(--border)]/50 px-2 py-2.5 text-left transition ${
                  current?.name === table.name
                    ? "bg-[var(--accent)] text-[var(--foreground)]"
                    : "text-[var(--muted-foreground)] hover:bg-[var(--accent)] hover:text-[var(--foreground)]"
                }`}
                onClick={() => onActiveTableChange(table.name)}
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="truncate font-medium whitespace-nowrap">{formatTableTitle(table)}</div>
                  <Badge variant={current?.name === table.name ? "default" : "outline"}>{(table.rows || []).length}</Badge>
                </div>
                <div className="mt-1 truncate text-xs text-[var(--muted-foreground)]">{formatTableDescription(table) || previewColumns(table.columns)}</div>
              </button>
            ))}
            {!tables.length ? <div className="text-sm text-[var(--muted-foreground)]">当前没有数据表。</div> : null}
          </div>
        </CardContent>
      </Card>

      <div className="space-y-5">
      <Card className="bg-transparent">
        <CardHeader className="border-b border-[var(--border)]/60 pb-3">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <CardTitle>{current ? formatTableTitle(current) : "未选择数据表"}</CardTitle>
              <CardDescription>{current ? formatTableDescription(current) || "更像内容库，而不是传统数据库表格。" : "更像内容库，而不是传统数据库表格。"}</CardDescription>
            </div>
            {current ? (
              <div className="flex flex-wrap gap-2">
                <Badge variant="outline">{(current.rows || []).length} 条记录</Badge>
                <Badge variant="outline">{getVisibleColumns(current).length} 个字段</Badge>
                {getHiddenMetaColumns(current).length ? <Badge variant="outline">隐藏 {getHiddenMetaColumns(current).length} 个系统字段</Badge> : null}
              </div>
            ) : null}
          </div>
        </CardHeader>
        <CardContent>
          {current ? (
            <div className="space-y-4">
              <div className="flex gap-2">
                <Button variant={viewMode === "table" ? "default" : "outline"} size="sm" onClick={() => setViewMode("table")}>
                  表格视图
                </Button>
                <Button variant={viewMode === "graph" ? "default" : "outline"} size="sm" onClick={() => setViewMode("graph")}>
                  节点视图
                </Button>
              </div>
              {viewMode === "table" ? (
                <DataTable
                  columns={tableColumns}
                  data={filteredRows}
                  filterColumnId={heroColumn}
                  filterPlaceholder={`搜索${formatColumnLabel(heroColumn)}`}
                  pageSize={12}
                  selectedRowId={rowIdentity}
                  onRowClick={(row) => {
                    const nextIndex = filteredRows.findIndex((item) => item === row);
                    setSelectedRowIndex(nextIndex >= 0 ? nextIndex : 0);
                  }}
                  emptyText="当前没有可展示的记录。"
                />
              ) : (
                <NodeGraphView
                  centerLabel={formatDisplayValue(selectedRow?.[heroColumn] || selectedRow?.title || selectedRow?.name || selectedRow?.id || "未命名", referenceMap)}
                  connections={selectedConnections}
                  onSelectConnection={(connection) => {
                    onActiveTableChange(connection.targetTableName);
                    setPendingTargetId(connection.targetId);
                  }}
                />
              )}
            </div>
          ) : (
            <div className="text-sm text-[var(--muted-foreground)]">未选择数据表。</div>
          )}
        </CardContent>
      </Card>

      <Card className="bg-transparent">
        <CardHeader className="border-b border-[var(--border)]/60">
          <CardTitle>节点详情</CardTitle>
          <CardDescription>查看当前记录、关联节点与来源片段。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedRow && current ? (
            <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_320px]">
              <div className="space-y-4">
                  <div className="truncate text-lg font-semibold">
                    {formatDisplayValue(selectedRow[heroColumn] || selectedRow.title || selectedRow.name || selectedRow.id || "未命名", referenceMap)}
                  </div>
                <div className="grid gap-3 md:grid-cols-2">
                  {getVisibleColumns(current).map((column) => (
                    <div key={`detail-${column}`} className="border-b border-[var(--border)]/60 py-2 text-sm">
                      <div className="truncate text-[var(--muted-foreground)] whitespace-nowrap">{formatColumnLabel(column)}</div>
                      <div className="mt-1 break-words">{formatDisplayValue(selectedRow[column], referenceMap) || "—"}</div>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-5">
              <div>
                <div className="text-sm font-medium">关联节点</div>
                <div className="mt-3 space-y-2">
                  {selectedConnections.length ? (
                    selectedConnections.map((connection, index) => (
                      <button
                        key={`conn-${index}`}
                        className="w-full border-b border-[var(--border)]/60 px-0 py-3 text-left transition hover:text-[var(--primary)]"
                        onClick={() => {
                          onActiveTableChange(connection.targetTableName);
                          setPendingTargetId(connection.targetId);
                        }}
                      >
                        <div className="truncate text-sm font-medium">{connection.label}</div>
                        <div className="mt-1 text-xs text-[var(--muted-foreground)]">{connection.description}</div>
                      </button>
                    ))
                  ) : (
                    <div className="text-sm text-[var(--muted-foreground)]">当前没有识别到明确的关联节点。</div>
                  )}
                </div>
              </div>

              <div>
                <div className="text-sm font-medium">来源片段</div>
                <div className="mt-3 space-y-2">
                  {selectedSources.length ? (
                    selectedSources.map((source, index) => (
                      <div key={`source-${index}`} className="border-b border-[var(--border)]/60 px-0 py-3">
                        <div className="truncate text-sm font-medium">{source.title}</div>
                        <div className="mt-1 text-xs text-[var(--muted-foreground)]">{source.meta}</div>
                        <div className="mt-2 text-sm">{source.summary}</div>
                      </div>
                    ))
                  ) : (
                    <div className="text-sm text-[var(--muted-foreground)]">当前没有可用的来源片段。</div>
                  )}
                </div>
              </div>
              </div>
            </div>
          ) : (
            <div className="text-sm text-[var(--muted-foreground)]">请选择一条记录查看详情。</div>
          )}
        </CardContent>
      </Card>
      </div>
    </div>
  );
}

function NodeWorkspaceView({ workspaceId, tables, recentSources }) {
  const [nodeQuery, setNodeQuery] = useState("");
  const [tableFilter, setTableFilter] = useState("all");
  const [selectedNodeId, setSelectedNodeId] = useState("");
  const [graphMode, setGraphMode] = useState("lane");
  const referenceMap = useMemo(() => buildReferenceMap(tables), [tables]);

  const allNodes = useMemo(() => {
    const items = [];
    for (const table of tables || []) {
      for (const row of table.rows || []) {
        const id = row.id || row.entity_id || row.event_id || row.claim_id || row.relation_id;
        if (!id) continue;
        items.push({
          id,
          label: formatDisplayValue(row.title || row.name || row.summary || row.description || id, referenceMap),
          tableName: table.name,
          tableTitle: formatTableTitle(table),
          row,
          table,
        });
      }
    }
    return items;
  }, [tables, referenceMap]);

  const filteredNodes = useMemo(() => {
    const normalized = nodeQuery.trim().toLowerCase();
    return allNodes.filter((node) => {
      const tablePass = tableFilter === "all" || node.tableName === tableFilter;
      const queryPass =
        !normalized ||
        JSON.stringify({
          label: node.label,
          tableTitle: node.tableTitle,
          row: node.row,
        })
          .toLowerCase()
          .includes(normalized);
      return tablePass && queryPass;
    });
  }, [allNodes, tableFilter, nodeQuery]);

  const graphEdges = useMemo(() => buildWorkspaceEdges(filteredNodes, tables, referenceMap), [filteredNodes, tables, referenceMap]);

  const selectedNode =
    filteredNodes.find((node) => node.id === selectedNodeId) ||
    allNodes.find((node) => node.id === selectedNodeId) ||
    filteredNodes[0] ||
    null;

  useEffect(() => {
    if (!selectedNode && filteredNodes[0]) {
      setSelectedNodeId(filteredNodes[0].id);
    }
  }, [filteredNodes, selectedNode]);

  const selectedConnections = selectedNode
    ? buildRowConnections(selectedNode.row, selectedNode.table, tables, referenceMap)
    : [];
  const selectedSources = selectedNode ? buildRowSourceSnippets(selectedNode.row, recentSources) : [];
  const nodeTableColumns = [
    {
      accessorKey: "label",
      header: "节点名称",
      cell: ({ row }) => <div className="font-medium">{row.original?.label || "未命名节点"}</div>,
    },
    {
      accessorKey: "tableTitle",
      header: "所属类型",
      cell: ({ row }) => (
        <span style={{ color: getNodeColor(row.original?.tableName).accent }}>{row.original?.tableTitle || "-"}</span>
      ),
    },
    {
      accessorKey: "relations",
      header: "关联数",
      cell: ({ row }) =>
        buildRowConnections(row.original?.row, row.original?.table, tables, referenceMap).length,
    },
  ];

  return (
    <div className="grid gap-5 xl:grid-cols-[240px_minmax(0,1fr)]">
      <Card className="border-r border-[var(--border)]/70 bg-transparent">
        <CardHeader>
          <CardTitle>分析入口</CardTitle>
          <CardDescription>跨表聚合全部数据表，方便做结构分析与关系探索。</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <Select value={tableFilter} onChange={(event) => setTableFilter(event.target.value)}>
            <option value="all">全部数据表</option>
            {tables.map((table) => (
              <option key={table.name} value={table.name}>
                {formatTableTitle(table)}
              </option>
            ))}
          </Select>
          <Input value={nodeQuery} onChange={(event) => setNodeQuery(event.target.value)} placeholder="搜索节点、字段或内容" />
          <div className="border-t border-[var(--border)]/60 pt-3 text-[11px] text-[var(--muted-foreground)]">
            工作区：{workspaceId || "-"} · 共 {filteredNodes.length} 个节点
          </div>
          <ScrollArea className="h-[540px]">
            <div className="space-y-1">
              {filteredNodes.map((node) => (
                <button
                  key={node.id}
                  className={`w-full border-b border-[var(--border)]/50 px-2 py-2 text-left transition ${
                    selectedNode?.id === node.id ? "bg-[var(--accent)] text-[var(--foreground)]" : "text-[var(--muted-foreground)] hover:bg-[var(--accent)] hover:text-[var(--foreground)]"
                  }`}
                  onClick={() => setSelectedNodeId(node.id)}
                >
                  <div className="truncate text-xs font-medium">{node.label}</div>
                  <div className="mt-1 text-[11px]">{node.tableTitle}</div>
                </button>
              ))}
              {!filteredNodes.length ? <div className="px-2 py-4 text-xs text-[var(--muted-foreground)]">当前筛选条件下没有节点。</div> : null}
            </div>
          </ScrollArea>
        </CardContent>
      </Card>

      <div className="space-y-5">
        <Card className="bg-transparent">
          <CardHeader className="border-b border-[var(--border)]/60 pb-3">
            <CardTitle>{selectedNode ? selectedNode.label : "分析视图"}</CardTitle>
            <CardDescription>{selectedNode ? `${selectedNode.tableTitle} · 全表聚合关系分析` : "聚合全部表的节点与关系，支持拖拽、筛选和 3D 图谱"}</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="mb-4 flex gap-2">
              <Button variant={graphMode === "lane" ? "default" : "outline"} size="sm" onClick={() => setGraphMode("lane")}>
                泳道图
              </Button>
              <Button variant={graphMode === "adjacency" ? "default" : "outline"} size="sm" onClick={() => setGraphMode("adjacency")}>
                邻接图
              </Button>
              <Button variant={graphMode === "3d" ? "default" : "outline"} size="sm" onClick={() => setGraphMode("3d")}>
                3D 图谱
              </Button>
              <Button variant={graphMode === "table" ? "default" : "outline"} size="sm" onClick={() => setGraphMode("table")}>
                数据表
              </Button>
            </div>
            {filteredNodes.length ? (
              graphMode === "3d" ? (
                <WorkspaceGraph3DView
                  nodes={filteredNodes}
                  edges={graphEdges}
                  selectedNodeId={selectedNode?.id || ""}
                  onSelectNode={setSelectedNodeId}
                />
              ) : graphMode === "adjacency" ? (
                <NodeGraphView
                  centerLabel={selectedNode?.label || "当前节点"}
                  connections={selectedConnections}
                  onSelectConnection={(connection) => setSelectedNodeId(connection.targetId)}
                />
              ) : graphMode === "table" ? (
                <DataTable
                  columns={nodeTableColumns}
                  data={filteredNodes}
                  filterColumnId="label"
                  filterPlaceholder="搜索节点名称"
                  pageSize={16}
                  selectedRowId={selectedNode?.id || ""}
                  onRowClick={(row) => setSelectedNodeId(row.id)}
                  emptyText="当前没有可展示的节点。"
                />
              ) : (
                <WorkspaceGraphView
                  nodes={filteredNodes}
                  edges={graphEdges}
                  selectedNodeId={selectedNode?.id || ""}
                  onSelectNode={setSelectedNodeId}
                />
              )
            ) : (
              <div className="text-xs text-[var(--muted-foreground)]">暂无可展示节点。</div>
            )}
          </CardContent>
        </Card>

        <Card className="bg-transparent">
          <CardHeader className="border-b border-[var(--border)]/60 pb-3">
            <CardTitle>分析详情</CardTitle>
            <CardDescription>点击节点后在这里查看详细字段、关系和来源。</CardDescription>
          </CardHeader>
          <CardContent>
            {selectedNode ? (
              <div className="grid gap-6 xl:grid-cols-[minmax(0,1fr)_320px]">
                <div className="space-y-4">
                  <div className="border-b border-[var(--border)]/60 pb-3">
                    <div className="truncate text-base font-semibold">{selectedNode.label}</div>
                    <div className="mt-1 text-xs text-[var(--muted-foreground)]">{selectedNode.tableTitle}</div>
                  </div>
                  <div className="grid gap-3 md:grid-cols-2">
                    {getVisibleColumns(selectedNode.table).map((column) => (
                      <div key={`${selectedNode.id}-${column}`} className="border-b border-[var(--border)]/60 py-2 text-xs">
                        <div className="truncate text-[var(--muted-foreground)] whitespace-nowrap">{formatColumnLabel(column)}</div>
                        <div className="mt-1 break-words">{formatDisplayValue(selectedNode.row[column], referenceMap) || "—"}</div>
                      </div>
                    ))}
                  </div>
                </div>
                <div className="space-y-5">
                  <div>
                    <div className="text-xs font-medium">关联节点</div>
                    <div className="mt-3 space-y-2">
                      {selectedConnections.length ? (
                        selectedConnections.map((connection, index) => (
                          <button
                            key={`${connection.targetId}-${index}`}
                            className="w-full border-b border-[var(--border)]/60 px-0 py-2.5 text-left transition hover:text-[var(--primary)]"
                            onClick={() => setSelectedNodeId(connection.targetId)}
                          >
                            <div className="truncate text-xs font-medium">{connection.label}</div>
                            <div className="mt-1 text-[11px] text-[var(--muted-foreground)]">{connection.description}</div>
                          </button>
                        ))
                      ) : (
                        <div className="text-xs text-[var(--muted-foreground)]">当前没有识别到明确的关联节点。</div>
                      )}
                    </div>
                  </div>

                  <div>
                    <div className="text-xs font-medium">来源片段</div>
                    <div className="mt-3 space-y-2">
                      {selectedSources.length ? (
                        selectedSources.map((source, index) => (
                          <div key={`${source.title}-${index}`} className="border-b border-[var(--border)]/60 px-0 py-2.5">
                            <div className="truncate text-xs font-medium">{source.title}</div>
                            <div className="mt-1 text-[11px] text-[var(--muted-foreground)]">{source.meta}</div>
                            <div className="mt-2 text-xs">{source.summary}</div>
                          </div>
                        ))
                      ) : (
                        <div className="text-xs text-[var(--muted-foreground)]">当前没有可用的来源片段。</div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            ) : (
              <div className="text-xs text-[var(--muted-foreground)]">请选择节点查看详情。</div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function WorkspaceGraphView({ nodes, edges, selectedNodeId, onSelectNode }) {
  const containerRef = useRef(null);
  const dragRef = useRef(null);
  const [positions, setPositions] = useState({});
  const [viewport, setViewport] = useState({ width: 920, height: 560 });
  const width = viewport.width;
  const height = viewport.height;
  const groupedTables = useMemo(() => {
    const order = [];
    const buckets = new Map();
    nodes.forEach((node) => {
      if (!buckets.has(node.tableName)) {
        buckets.set(node.tableName, []);
        order.push(node.tableName);
      }
      buckets.get(node.tableName).push(node);
    });
    return order.map((tableName) => ({
      tableName,
      items: buckets.get(tableName) || [],
    }));
  }, [nodes]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container || typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      const nextWidth = Math.max(760, Math.floor(entry.contentRect.width));
      const nextHeight = Math.max(560, Math.floor(entry.contentRect.height));
      setViewport((current) =>
        current.width === nextWidth && current.height === nextHeight
          ? current
          : { width: nextWidth, height: nextHeight },
      );
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    setPositions((current) => {
      const next = { ...current };
      const sectionCount = Math.max(1, groupedTables.length);
      const topPadding = 72;
      const bottomPadding = 56;
      const usableHeight = Math.max(120, height - topPadding - bottomPadding);

      groupedTables.forEach((group, groupIndex) => {
        const columnX = sectionCount === 1 ? width / 2 : 110 + (groupIndex * (width - 220)) / Math.max(1, sectionCount - 1);
        const count = Math.max(1, group.items.length);
        group.items.forEach((node, index) => {
          if (next[node.id]) return;
          const rowGap = usableHeight / Math.max(1, count);
          next[node.id] = {
            x: clamp(columnX + ((index % 2 === 0 ? -1 : 1) * Math.min(24, 8 + Math.floor(index / 2) * 5)), 86, width - 86),
            y: clamp(topPadding + rowGap * index + rowGap / 2, 52, height - 52),
          };
        });
      });

      nodes.forEach((node) => {
        if (next[node.id]) return;
        next[node.id] = {
          x: width / 2,
          y: height / 2,
        };
      });

      Object.keys(next).forEach((key) => {
        if (!nodes.some((node) => node.id === key)) {
          delete next[key];
        }
      });

      return next;
    });
  }, [nodes, groupedTables, width, height]);

  useEffect(() => {
    const handlePointerMove = (event) => {
      const active = dragRef.current;
      const container = containerRef.current;
      if (!active || !container) return;
      const bounds = container.getBoundingClientRect();
      const x = clamp(event.clientX - bounds.left, 72, bounds.width - 72);
      const y = clamp(event.clientY - bounds.top, 40, bounds.height - 40);
      setPositions((current) => ({
        ...current,
        [active.nodeId]: { x, y },
      }));
    };

    const handlePointerUp = () => {
      dragRef.current = null;
    };

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", handlePointerUp);
    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", handlePointerUp);
    };
  }, []);

  const edgeElements = edges.map((edge, index) => {
    const source = positions[edge.sourceId];
    const target = positions[edge.targetId];
    if (!source || !target) return null;
    return (
      <line
        key={`${edge.sourceId}-${edge.targetId}-${index}`}
        x1={source.x}
        y1={source.y}
        x2={target.x}
        y2={target.y}
        stroke="color-mix(in oklab, var(--foreground) 16%, var(--border))"
        strokeWidth="1"
        strokeOpacity={edge.sourceId === selectedNodeId || edge.targetId === selectedNodeId ? "0.9" : "0.5"}
      />
    );
  });

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--border)]/60 pb-2 text-[11px] text-[var(--muted-foreground)]">
        <span>{nodes.length} 个节点</span>
        <span>·</span>
        <span>{edges.length} 条连线</span>
        <span>·</span>
        <span>拖拽节点可调整布局，点击节点可查看详情</span>
      </div>
      <div className="flex flex-wrap items-center gap-3 border-b border-[var(--border)]/60 pb-2 text-[11px] text-[var(--muted-foreground)]">
        {groupedTables.map((group) => (
          <div key={group.tableName} className="flex items-center gap-1.5 whitespace-nowrap">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
              style={{ background: getNodeColor(group.tableName).accent }}
            />
            <span>{formatTableTitle({ name: group.tableName })}</span>
            <span>({group.items.length})</span>
          </div>
        ))}
      </div>
      <div
        ref={containerRef}
        className="relative overflow-hidden rounded-[calc(var(--radius)+0.06rem)] border border-[var(--border)]/70 bg-[linear-gradient(180deg,color-mix(in_oklab,var(--background)_95%,var(--secondary)),var(--background))]"
        style={{ height }}
      >
        {groupedTables.map((group, index) => {
          const sectionCount = Math.max(1, groupedTables.length);
          const left = sectionCount === 1 ? 0 : (index * 100) / sectionCount;
          const widthPercent = 100 / sectionCount;
          return (
            <div
              key={`lane-${group.tableName}`}
              className="absolute inset-y-0 border-r border-[var(--border)]/45"
              style={{
                left: `${left}%`,
                width: `${widthPercent}%`,
                background:
                  index % 2 === 0
                    ? "color-mix(in oklab, var(--accent) 18%, transparent)"
                    : "transparent",
              }}
            >
              <div className="px-3 py-2 text-[10px] font-medium uppercase tracking-[0.14em] text-[var(--muted-foreground)]">
                {formatTableTitle({ name: group.tableName })}
              </div>
            </div>
          );
        })}
        <svg className="absolute inset-0 h-full w-full" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
          {edgeElements}
        </svg>

        {nodes.map((node) => {
          const position = positions[node.id];
          if (!position) return null;
          const selected = node.id === selectedNodeId;
          const tone = getNodeColor(node.tableName);
          return (
            <button
              key={node.id}
              className="absolute z-10 w-[124px] -translate-x-1/2 -translate-y-1/2 border px-2.5 py-2 text-left text-[11px] shadow-sm transition"
              style={{ left: position.x, top: position.y }}
              onClick={() => onSelectNode(node.id)}
              onPointerDown={(event) => {
                event.preventDefault();
                dragRef.current = { nodeId: node.id };
                onSelectNode(node.id);
              }}
              aria-pressed={selected}
            >
              <div
                className="absolute inset-0"
                style={{
                  borderColor: selected ? tone.accent : tone.border,
                  background: selected ? tone.soft : "var(--card)",
                }}
              />
              <div className="relative">
                <div className="truncate font-medium" style={{ color: selected ? tone.strong : "var(--foreground)" }}>
                  {node.label}
                </div>
                <div className="mt-1 truncate text-[10px]" style={{ color: tone.accent }}>
                  {node.tableTitle}
                </div>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function WorkspaceGraph3DView({ nodes, edges, selectedNodeId, onSelectNode }) {
  const graphData = useMemo(
    () => ({
      nodes: nodes.map((node) => {
        const tone = getNodeColor(node.tableName);
        return {
          id: node.id,
          name: node.label,
          tableTitle: node.tableTitle,
          color: tone.accent,
          val: node.id === selectedNodeId ? 10 : 6,
        };
      }),
      links: edges.map((edge) => ({
        source: edge.sourceId,
        target: edge.targetId,
        label: edge.label,
      })),
    }),
    [nodes, edges, selectedNodeId],
  );

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2 border-b border-[var(--border)]/60 pb-2 text-[11px] text-[var(--muted-foreground)]">
        <span>{nodes.length} 个节点</span>
        <span>·</span>
        <span>{edges.length} 条连线</span>
        <span>·</span>
        <span>支持旋转、缩放和拖动视角，点击节点可查看详情</span>
      </div>
      <div className="overflow-hidden rounded-[calc(var(--radius)+0.06rem)] border border-[var(--border)]/70 bg-[linear-gradient(180deg,color-mix(in_oklab,var(--background)_95%,var(--secondary)),var(--background))]">
        <Suspense fallback={<div className="px-4 py-16 text-center text-xs text-[var(--muted-foreground)]">正在加载 3D 图谱...</div>}>
          <ForceGraph3D
            graphData={graphData}
            width={960}
            height={560}
            backgroundColor="rgba(0,0,0,0)"
            nodeLabel={(node) => `${node.name} · ${node.tableTitle}`}
            nodeColor={(node) => node.color}
            nodeRelSize={5}
            linkOpacity={0.28}
            linkWidth={(link) =>
              link.source?.id === selectedNodeId || link.target?.id === selectedNodeId ? 2.4 : 1
            }
            linkColor={(link) =>
              link.source?.id === selectedNodeId || link.target?.id === selectedNodeId
                ? "rgba(234,88,12,0.88)"
                : "rgba(100,116,139,0.42)"
            }
            onNodeClick={(node) => onSelectNode(node.id)}
            cooldownTicks={90}
          />
        </Suspense>
      </div>
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
  const recentTrace = trace.slice(0, 5);

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricCard label="业务表" value={tables.length} delta={`系统表 ${systemCount} 张`} hint="当前知识视图数量" icon={Database} />
        <MetricCard label="记录总数" value={totalRows} delta={`已完成 ${completedTasks}`} hint="全部业务表中的记录汇总" icon={FileText} />
        <MetricCard label="运行中任务" value={activeTasks} delta={latestTask ? `最近：${formatStepLabel(latestTask.current_step || "-")}` : "暂无任务"} hint="建议同一工作区串行执行" icon={Activity} />
        <MetricCard label="异常任务" value={staleTasks} delta={staleTasks ? "需要关注" : "状态正常"} hint="阻塞、失败或长时间无新进度" icon={GitBranch} />
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
        <Card>
          <CardHeader>
            <CardTitle>工作区摘要</CardTitle>
            <CardDescription>像一个运营工作台，先看状态，再进入具体表与任务。</CardDescription>
          </CardHeader>
          <CardContent className="space-y-5">
            <div className="grid gap-3 md:grid-cols-2">
              <SummaryRow label="工作区" value={workspaceId || "-"} />
              <SummaryRow label="知识域" value={domain || "未提供"} />
              <SummaryRow label="最近任务" value={latestTask?.task_id || "暂无"} />
              <SummaryRow label="当前阶段" value={latestTask ? formatStepLabel(latestTask.current_step || "-") : "-"} />
            </div>
            <div className="rounded-[calc(var(--radius)+0.35rem)] border bg-[var(--background)] p-5">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="text-xl font-semibold">工作区活跃度</div>
                  <div className="mt-1 text-sm text-[var(--muted-foreground)]">过去几轮产出与流程状态的快速概览</div>
                </div>
                <div className="flex gap-2">
                  <Badge variant="outline">表 {tables.length}</Badge>
                  <Badge variant="outline">记录 {totalRows}</Badge>
                </div>
              </div>
              <div className="mt-6 grid grid-cols-[1fr_auto] gap-6">
                <div className="grid h-48 grid-cols-7 items-end gap-3">
                {[activeTasks, completedTasks, staleTasks, tables.length, totalRows % 12 || 4, topTables.length || 1, (trace.length % 9) + 2].map((value, index) => (
                  <div key={index} className="flex h-full flex-col justify-end gap-2">
                    <div
                      className="rounded-t-[14px] bg-[linear-gradient(180deg,color-mix(in_oklab,var(--primary)_70%,white),var(--primary))]"
                      style={{ height: `${Math.max(18, Math.min(100, value * 12))}%` }}
                    />
                    <div className="text-center text-xs text-[var(--muted-foreground)]">{["运行", "完成", "异常", "表", "活跃", "重点", "轨迹"][index]}</div>
                  </div>
                ))}
                </div>
                <div className="grid content-start gap-3">
                  <div className="rounded-[calc(var(--radius)+0.1rem)] border bg-[var(--card)] px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-[var(--muted-foreground)]">最新任务</div>
                    <div className="mt-2 text-sm font-medium">{latestTask?.task_id || "暂无"}</div>
                  </div>
                  <div className="rounded-[calc(var(--radius)+0.1rem)] border bg-[var(--card)] px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-[var(--muted-foreground)]">当前阶段</div>
                    <div className="mt-2 text-sm font-medium">{latestTask ? formatStepLabel(latestTask.current_step || "-") : "-"}</div>
                  </div>
                  <div className="rounded-[calc(var(--radius)+0.1rem)] border bg-[var(--card)] px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-[var(--muted-foreground)]">空间状态</div>
                    <div className="mt-2 text-sm font-medium">{staleTasks ? "需要人工关注" : "运行稳定"}</div>
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>重点数据表</CardTitle>
            <CardDescription>优先展示记录最多、最值得继续操作的几张表。</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
              {topTables.length ? (
              topTables.map((table, index) => (
                <div key={table.name} className="rounded-[calc(var(--radius)+0.25rem)] border bg-[var(--background)] px-4 py-4">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="font-medium">{table.title || table.name}</div>
                      <div className="mt-1 text-xs text-[var(--muted-foreground)]">{table.name}</div>
                    </div>
                    <Badge variant={index === 0 ? "default" : "outline"}>{(table.rows || []).length} 行</Badge>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2 text-xs text-[var(--muted-foreground)]">
                    <span>{(table.columns || []).length} 列</span>
                    <span>·</span>
                    <span>{previewColumns(table.columns)}</span>
                  </div>
                </div>
              ))
            ) : (
              <div className="text-sm text-[var(--muted-foreground)]">当前还没有数据表结构。</div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
      <Card>
        <CardHeader>
          <CardTitle>最近智能体轨迹</CardTitle>
          <CardDescription>更像团队活动流，先看最近发生了什么。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {trace.slice(0, 6).map((entry, index) => (
              <div key={`${entry.timestamp}-${index}`} className="rounded-[calc(var(--radius)+0.25rem)] border bg-[var(--background)] px-4 py-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="font-medium">{formatAgentLabel(entry.agent || "") || formatTraceEventLabel(entry.event)}</div>
                  <Badge variant="outline">{formatTraceEventLabel(entry.event || "轨迹")}</Badge>
                </div>
                <div className="mt-2 text-xs text-[var(--muted-foreground)]">{entry.timestamp || "-"}</div>
                <div className="mt-1 text-sm">{formatStepLabel(entry.action || entry.step || "")}</div>
              </div>
            ))}
            {!trace.length ? <div className="text-sm text-[var(--muted-foreground)]">暂无智能体轨迹。</div> : null}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>最近任务</CardTitle>
          <CardDescription>像右侧运营侧栏，快速看是否需要介入。</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {latestTask ? (
            <>
              <SummaryRow label="任务编号" value={latestTask.task_id} />
              <SummaryRow label="当前状态" value={mapTaskStatus(getEffectiveTaskStatus(latestTask))} />
              <SummaryRow label="当前步骤" value={formatStepLabel(latestTask.current_step || "-")} />
              <SummaryRow label="最后提示" value={latestTask.resume_hint || "暂无"} />
            </>
          ) : (
            <div className="text-sm text-[var(--muted-foreground)]">暂无任务。</div>
          )}
          <div className="rounded-[calc(var(--radius)+0.25rem)] border bg-[var(--background)] p-4">
            <div className="text-sm font-medium">最近事件</div>
            <div className="mt-3 space-y-3">
              {recentTrace.length ? recentTrace.map((entry, index) => (
                <div key={`${entry.timestamp}-${index}-side`} className="flex gap-3">
                  <div className="mt-1 h-2.5 w-2.5 rounded-full bg-[var(--primary)]" />
                  <div className="min-w-0">
                    <div className="text-sm font-medium">{formatAgentLabel(entry.agent || "") || formatTraceEventLabel(entry.event)}</div>
                    <div className="text-xs text-[var(--muted-foreground)]">{formatStepLabel(entry.action || entry.step || "")}</div>
                  </div>
                </div>
              )) : <div className="text-sm text-[var(--muted-foreground)]">暂无事件。</div>}
            </div>
          </div>
        </CardContent>
      </Card>
      </div>
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
  const latestTaskStatus = getEffectiveTaskStatus(latestTask);
  const recentSourceColumns = [
    {
      accessorKey: "source_id",
      header: "来源",
      cell: ({ row }) => <div className="font-medium">{row.original?.source_id || "未命名来源"}</div>,
    },
    {
      accessorKey: "source_type",
      header: "类型",
      cell: ({ row }) => row.original?.source_type || "doc",
    },
    {
      accessorKey: "note",
      header: "备注",
      cell: ({ row }) => row.original?.context_hints?.note || "—",
    },
    {
      accessorKey: "ingested_at",
      header: "录入时间",
      cell: ({ row }) => row.original?.ingested_at || "—",
    },
    {
      accessorKey: "content",
      header: "内容摘要",
      cell: ({ row }) => <div className="max-w-[520px] truncate text-[var(--muted-foreground)]">{row.original?.content || "无内容"}</div>,
    },
  ];
  return (
    <Card className="mb-6">
      <CardHeader>
        <CardTitle>输入</CardTitle>
        <CardDescription>资料输入单独放在这个标签页。支持文本、文件、JSON 列表和备注。归入哪些数据表由 AI 自动判断。</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid gap-4 lg:grid-cols-2">
          <label className="space-y-2 text-sm text-[var(--muted-foreground)]">
            <span>纯文本</span>
            <Textarea
              className="min-h-28 rounded-2xl"
              placeholder="粘贴原始资料文本"
              value={textInput}
              onChange={(event) => onTextChange(event.target.value)}
            />
          </label>

          <label className="space-y-2 text-sm text-[var(--muted-foreground)]">
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
          <label className="space-y-2 text-sm text-[var(--muted-foreground)]">
            <span>上传文件</span>
            <Input
              type="file"
              className="h-auto rounded-2xl py-3"
              onChange={(event) => onFileChange(event.target.files?.[0] || null)}
            />
          </label>
        </div>

        <div className="mt-4 grid gap-4 lg:grid-cols-[1fr_auto] lg:items-end">
          <label className="space-y-2 text-sm text-[var(--muted-foreground)]">
            <span>备注</span>
            <Input
              className="h-12 rounded-2xl"
              value={noteField}
              onChange={(event) => onNoteChange(event.target.value)}
              placeholder="可选备注，会作为 AI 归档判断的补充提示"
            />
          </label>
          <Button onClick={onSubmit} disabled={latestTaskStatus === "running"}>
            {latestTaskStatus === "running" ? "任务运行中" : "提交资料"}
          </Button>
        </div>

        {ingestStatus ? (
          <div className="mt-4 border-t border-[var(--border)]/70 pt-4">
            <div className={`text-sm ${ingestStatus.ok ? "text-[var(--primary)]" : ingestStatus.loading ? "text-[var(--muted-foreground)]" : "text-[var(--destructive)]"}`}>
              {ingestStatus.message}
            </div>
            {latestTaskStatus === "running" ? (
              <div className="mt-2 text-xs text-[var(--muted-foreground)]">同一个工作区当前只建议串行运行一个任务，避免同时写同一批产物文件。</div>
            ) : null}
            <div className="mt-2 text-xs text-[var(--muted-foreground)]">系统会根据内容和备注自动判断归入哪些业务表，不再手动选表。</div>
            {latestTask ? (
              <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-[var(--muted-foreground)]">
                <Badge variant="outline">{latestTask.task_id}</Badge>
                <Badge variant="outline">{mapTaskStatus(latestTaskStatus)}</Badge>
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
              <div className="text-sm font-medium text-[var(--foreground)]">最近写入来源</div>
              <div className="text-xs text-[var(--muted-foreground)]">提交成功后，新增 source 会先出现在这里，再进入后续数据表与智能体流程。</div>
            </div>
            <Badge variant="outline">{recentSources.length} 条</Badge>
          </div>

          {recentSources.length ? (
            <DataTable
              columns={recentSourceColumns}
              data={recentSources}
              filterColumnId="source_id"
              filterPlaceholder="搜索来源"
              pageSize={6}
              emptyText="当前还没有最近写入记录。"
            />
          ) : (
            <div className="bg-[var(--background)] px-4 py-4 text-sm text-[var(--muted-foreground)]">当前还没有最近写入记录。</div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function TasksView({ tasks, selectedTask, selectedTaskId, onSelectTask, onDeleteTask, deleteStatus }) {
  const timeline = selectedTask?.stepTimeline || condenseStepEntries(selectedTask?.recentSteps || []);
  const coreTimeline = summarizeCoreTimeline(timeline, selectedTask);
  const eventLog = selectedTask?.stepEntries || selectedTask?.recentSteps || [];
  const liveStep = findLiveStep(selectedTask);
  const progress = selectedTask ? summarizeTaskProgress(selectedTask, coreTimeline) : null;

  return (
    <div className="grid gap-4 xl:grid-cols-[240px_1fr]">
      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>任务</CardTitle>
          <CardDescription>显示最近任务与状态。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-1">
            {tasks.length ? (
              tasks.map((task) => (
                <button
                  key={task.task_id}
                  className={`w-full border-b border-[var(--border)]/60 px-3 py-3 text-left transition ${
                    task.task_id === selectedTaskId
                      ? "bg-[var(--accent)] text-[var(--foreground)]"
                      : "text-[var(--muted-foreground)] hover:bg-[var(--accent)] hover:text-[var(--foreground)]"
                  }`}
                  onClick={() => onSelectTask(task.task_id)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="flex min-w-0 items-center gap-2">
                      {getEffectiveTaskStatus(task) === "running" ? (
                        <span className="mv-pulse-dot h-2 w-2 rounded-full bg-[var(--primary)]" />
                      ) : null}
                      <div className="truncate font-medium">{task.task_id}</div>
                    </div>
                    <StatusBadge task={task} />
                  </div>
                  <div className="mt-1 text-xs text-[var(--muted-foreground)]">
                    {mapTaskStatus(getEffectiveTaskStatus(task))} · {formatStepLabel(task.current_step || "-")}
                  </div>
                </button>
              ))
            ) : (
              <div className="text-sm text-[var(--muted-foreground)]">没有任务记录。</div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>任务详情</CardTitle>
          <CardDescription>单页查看任务状态、影响、核心流程和产物。</CardDescription>
        </CardHeader>
        <CardContent>
          {deleteStatus ? (
            <div className={`mb-4 text-sm ${deleteStatus.ok ? "text-[var(--primary)]" : "text-[var(--destructive)]"}`}>
              {deleteStatus.message}
            </div>
          ) : null}
          {selectedTask ? (
            <div className="space-y-6">
              <div className="border-b border-[var(--border)]/70 pb-4">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div>
                    <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">任务编号</div>
                    <div className="mt-2 text-xl font-semibold text-[var(--foreground)]">{selectedTask.task_id}</div>
                    <div className="mt-2 text-sm text-[var(--muted-foreground)]">{selectedTask.resume_hint || "暂无任务说明"}</div>
                  </div>
                  <div className="flex items-center gap-2">
                    <StatusBadge task={selectedTask} />
                    <Button size="sm" variant="outline" onClick={() => onDeleteTask?.(selectedTask.task_id)}>
                      删除任务
                    </Button>
                  </div>
                </div>
              </div>

              {progress ? (
                <div className="border-b border-[var(--border)]/70 pb-4">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">流程进度</div>
                      <div className={`mt-2 text-sm font-medium ${selectedTask.status === "running" ? "mv-shimmer-text" : "text-[var(--foreground)]"}`}>
                        {progress.label}
                      </div>
                    </div>
                    <div className="text-sm font-medium text-[var(--foreground)]">{progress.percent}%</div>
                  </div>
                  <div className="mt-3 h-2 rounded-full bg-[var(--secondary)]">
                    <div
                      className={`h-2 rounded-full bg-[var(--primary)] transition-all duration-500 ${selectedTask.status === "running" ? "mv-flow-bar" : ""}`}
                      style={{ width: `${Math.max(progress.percent, selectedTask.status === "running" ? 18 : 0)}%` }}
                    />
                  </div>
                  <div className="mt-2 text-xs text-[var(--muted-foreground)]">
                    已完成 {progress.completed} / {progress.total} 个核心阶段
                  </div>
                </div>
              ) : null}

              {hasTaskFailure(selectedTask) ? (
                <div className="border border-[var(--destructive)]/25 bg-[color-mix(in_oklab,var(--destructive)_8%,white)] px-4 py-4">
                  <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                      <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--destructive)]">失败原因</div>
                      <div className="mt-2 text-lg font-semibold text-[var(--foreground)]">{formatTaskFailureTitle(selectedTask)}</div>
                      <div className="mt-2 text-sm text-[var(--foreground)]">{formatTaskFailureMessage(selectedTask)}</div>
                    </div>
                    <Badge variant="danger">{mapTaskStatus(selectedTask.status)}</Badge>
                  </div>
                  <div className="mt-4 grid gap-3 md:grid-cols-3">
                    <FailureMeta label="失败阶段" value={formatStepLabel(selectedTask.current_step || findFailedStep(selectedTask)?.action || "-")} />
                    <FailureMeta label="失败智能体" value={formatAgentLabel(selectedTask.current_agent || findFailedStep(selectedTask)?.agent || "系统步骤")} />
                    <FailureMeta label="发生时间" value={formatTaskTime(selectedTask.ended_at || findFailedStep(selectedTask)?.timestamp)} />
                  </div>
                </div>
              ) : null}

              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                <TaskMetric label="状态" value={mapTaskStatus(getEffectiveTaskStatus(selectedTask))} task={selectedTask} />
                <TaskMetric label="当前步骤" value={formatStepLabel(selectedTask.current_step || "-")} />
                <TaskMetric label="当前智能体" value={formatAgentLabel(selectedTask.current_agent || "未指定")} />
                <TaskMetric label="最后心跳" value={formatTaskTime(selectedTask.last_heartbeat)} />
              </div>

              {selectedTask.status === "running" && liveStep ? (
                <div className="border border-[var(--ring)]/25 bg-[color-mix(in_oklab,var(--primary)_8%,white)] px-4 py-4">
                  <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div>
                      <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--primary)]">实时进度</div>
                      <div className="mt-2 text-lg font-semibold text-[var(--foreground)]">
                        {formatStepLabel(liveStep.action || selectedTask.current_step || "-")} 正在执行
                      </div>
                      <div className="mt-2 text-sm text-[var(--foreground)]">{liveStep.resume_hint || selectedTask.resume_hint || "正在处理，请稍候。"}</div>
                    </div>
                    <Badge variant="warm">运行中</Badge>
                  </div>
                  <div className="mt-4 grid gap-3 md:grid-cols-4">
                    <SummaryRow label="执行智能体" value={formatAgentLabel(liveStep.agent || selectedTask.current_agent || "系统步骤")} />
                    <SummaryRow label="开始时间" value={formatTaskTime(liveStep.started_at || liveStep.timestamp)} />
                    <SummaryRow label="已运行" value={formatElapsed(liveStep.started_at || liveStep.timestamp)} />
                    <SummaryRow label="最后心跳" value={formatTaskTime(selectedTask.last_heartbeat)} />
                  </div>
                </div>
              ) : null}

              <div className="border-t border-[var(--border)]/70 pt-4">
                <div className="text-sm font-medium text-[var(--foreground)]">本次更新影响</div>
                {selectedTask.impact?.source_count ? (
                  <div className="mt-3 space-y-3">
                    <div className="flex flex-wrap items-center gap-2 text-sm text-[var(--muted-foreground)]">
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
                          <div key={database.name} className="border-b border-[var(--border)]/60 px-0 py-3">
                            <div className="font-medium text-[var(--foreground)]">
                              {database.title} · {database.row_count} 条记录
                            </div>
                            <div className="mt-2 text-sm text-[var(--muted-foreground)]">
                              {(database.sample_rows || []).join(" / ") || "已合并到现有记录"}
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="text-sm text-[var(--muted-foreground)]">这次来源已进入任务，但还没有映射出明显的新数据表记录。</div>
                      )}
                    </div>
                  </div>
                ) : (
                  <div className="mt-2 text-sm text-[var(--muted-foreground)]">暂时还没有统计到本次任务对应的来源变更。</div>
                )}
              </div>

              <div>
                <div className="mb-3 text-sm font-medium text-[var(--foreground)]">核心流程</div>
                <div className="space-y-3">
                  {coreTimeline.length ? (
                    coreTimeline.map((step, index) => (
                      <div
                        key={`${step.timestamp || index}-${step.action || step.status}`}
                        className={`border-b border-[var(--border)]/60 px-0 py-4 ${
                          step.status === "running" ? "bg-[color-mix(in_oklab,var(--primary)_4%,transparent)]" : ""
                        }`}
                      >
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div className="flex items-center gap-2">
                            {step.status === "running" ? (
                              <span className="mv-pulse-dot h-2 w-2 rounded-full bg-[var(--primary)]" />
                            ) : null}
                            <div className="font-medium text-[var(--foreground)]">{step.label || formatStepLabel(step.action || "-")}</div>
                          </div>
                          <Badge variant={mapStepVariant(step.status)}>{mapStepStatus(step.status)}</Badge>
                        </div>
                        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-[var(--muted-foreground)]">
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
                        {step.resume_hint ? <div className={`mt-2 text-sm text-[var(--muted-foreground)] ${step.status === "running" ? "mv-shimmer-text" : ""}`}>{step.resume_hint}</div> : null}
                        {step.status === "running" ? (
                          <div className="mt-3 h-1.5 rounded-full bg-[var(--secondary)]">
                            <div className="mv-flow-bar h-1.5 w-1/3 rounded-full bg-[var(--primary)]" />
                          </div>
                        ) : null}
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
                          <div className="mt-3 text-xs text-[var(--muted-foreground)]">Chunk：{step.chunk_ids.join(" / ")}</div>
                        ) : null}
                        {step.actions?.length ? (
                          <div className="mt-3 text-xs text-[var(--muted-foreground)]">
                            包含步骤：{step.actions.map((name) => formatStepLabel(name)).join(" / ")}
                          </div>
                        ) : null}
                      </div>
                    ))
                  ) : (
                    <div className="px-0 py-4 text-sm text-[var(--muted-foreground)]">暂无核心流程记录。</div>
                  )}
                </div>
              </div>

              <div>
                <div className="mb-3 text-sm font-medium text-[var(--foreground)]">产物</div>
                <div className="grid gap-3 md:grid-cols-2">
                  {Object.entries(selectedTask.artifacts || {}).length ? (
                    Object.entries(selectedTask.artifacts || {}).map(([key, value]) => (
                      <div key={key} className="border-b border-[var(--border)]/60 px-0 py-3">
                        <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">{key}</div>
                        <div className="mt-1 break-all text-sm text-[var(--foreground)]">{String(value)}</div>
                      </div>
                    ))
                  ) : (
                    <div className="px-0 py-4 text-sm text-[var(--muted-foreground)]">暂无产物记录。</div>
                  )}
                </div>
              </div>

              <details className="border-t border-[var(--border)]/70 pt-4">
                <summary className="cursor-pointer text-sm font-medium text-[var(--foreground)]">高级日志</summary>
                <div className="mt-4">
                  <div className="mb-3 text-sm font-medium text-[var(--foreground)]">原始事件流</div>
                  <ScrollArea className="h-[320px] border border-[var(--border)]/70 bg-[var(--background)]">
                    <div className="space-y-3 p-4">
                      {eventLog.length ? (
                        eventLog.map((step, index) => (
                          <div key={`${step.timestamp || index}-${step.action || step.status}-raw`} className="border-b border-[var(--border)]/60 pb-3 last:border-b-0">
                            <div className="flex flex-wrap items-center justify-between gap-3">
                              <div className="font-medium text-[var(--foreground)]">
                                {formatStepLabel(step.action || "-")} · {mapStepStatus(step.status)}
                              </div>
                              <div className="text-xs text-[var(--muted-foreground)]">{formatTaskTime(step.timestamp)}</div>
                            </div>
                            <div className="mt-1 text-xs text-[var(--muted-foreground)]">{formatAgentLabel(step.agent || "系统步骤")}</div>
                            {step.resume_hint ? <div className="mt-2 text-sm text-[var(--muted-foreground)]">{step.resume_hint}</div> : null}
                            {Object.keys(step).length ? (
                              <pre className="mt-2 overflow-auto border border-[var(--border)]/60 bg-[var(--secondary)]/35 p-3 text-xs text-[var(--foreground)]">
                                {JSON.stringify(step, null, 2)}
                              </pre>
                            ) : null}
                          </div>
                        ))
                      ) : (
                        <div className="text-sm text-[var(--muted-foreground)]">暂无事件日志。</div>
                      )}
                    </div>
                  </ScrollArea>
                </div>
                <details className="mt-4 border border-[var(--border)]/70 bg-[var(--foreground)] px-4 py-4 text-[var(--background)]">
                  <summary className="cursor-pointer text-sm font-medium">查看完整 JSON</summary>
                  <pre className="mt-4 overflow-auto text-xs">{JSON.stringify(selectedTask, null, 2)}</pre>
                </details>
              </details>
            </div>
          ) : (
            <div className="text-sm text-[var(--muted-foreground)]">未选择任务。</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function SkillsView({ skills, selectedSkillId, onSelectSkill, onToggleAutoUpdate, onSyncSkill, status }) {
  const selectedSkill = skills.find((skill) => skill.id === selectedSkillId) || skills[0] || null;

  return (
    <div className="grid gap-4 xl:grid-cols-[240px_1fr]">
      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>技能</CardTitle>
          <CardDescription>项目级 Skill 管理、自动更新和同步。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-1">
            {skills.length ? (
              skills.map((skill) => (
                <button
                  key={skill.id}
                  className={`w-full border-b border-[var(--border)]/60 px-3 py-3 text-left transition ${
                    skill.id === selectedSkill?.id
                      ? "bg-[var(--accent)] text-[var(--foreground)]"
                      : "text-[var(--muted-foreground)] hover:bg-[var(--accent)] hover:text-[var(--foreground)]"
                  }`}
                  onClick={() => onSelectSkill(skill.id)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="truncate font-medium">{skill.title || skill.id}</div>
                    <Badge variant={skill.auto_update ? "default" : "outline"}>
                      {skill.auto_update ? "自动更新" : "手动"}
                    </Badge>
                  </div>
                  <div className="mt-2 text-xs text-[var(--muted-foreground)]">{skill.id}</div>
                </button>
              ))
            ) : (
              <div className="text-sm text-[var(--muted-foreground)]">当前没有已登记技能。</div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>技能设置</CardTitle>
          <CardDescription>查看来源、描述、自动更新状态，并手动同步。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedSkill ? (
            <div className="space-y-4">
              <div className="grid gap-3 md:grid-cols-2">
                <SummaryRow label="名称" value={selectedSkill.title || selectedSkill.id} />
                <SummaryRow label="标识" value={selectedSkill.id} />
                <SummaryRow label="来源" value={selectedSkill.source?.url || "-"} />
                <SummaryRow label="路径" value={selectedSkill.path || "-"} />
                <SummaryRow label="状态" value={selectedSkill.status || (selectedSkill.exists ? "installed" : "missing")} />
                <SummaryRow label="最近更新" value={selectedSkill.last_updated_at || "-"} />
              </div>

              <div className="border-t border-[var(--border)]/70 pt-4 text-sm text-[var(--muted-foreground)]">
                {selectedSkill.description || "没有描述。"}
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <Button
                  variant={selectedSkill.auto_update ? "default" : "outline"}
                  onClick={() => onToggleAutoUpdate(selectedSkill.id, !selectedSkill.auto_update)}
                >
                  {selectedSkill.auto_update ? "关闭自动更新" : "开启自动更新"}
                </Button>
                <Button variant="outline" onClick={() => onSyncSkill(selectedSkill.id)}>
                  立即同步
                </Button>
                {selectedSkill.manifestPath ? <Badge variant="outline">{selectedSkill.manifestPath}</Badge> : null}
              </div>

              {status ? (
                <div className={`text-sm ${status.ok ? "text-[var(--primary)]" : "text-[var(--destructive)]"}`}>
                  {status.message}
                </div>
              ) : null}

              {selectedSkill.last_error ? (
                <div className="border border-[var(--destructive)]/25 bg-[color-mix(in_oklab,var(--destructive)_8%,white)] px-4 py-4 text-sm text-[var(--destructive)]">
                  最近同步错误：{selectedSkill.last_error}
                </div>
              ) : null}
            </div>
          ) : (
            <div className="text-sm text-[var(--muted-foreground)]">未选择技能。</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function SettingsView({
  workspaceId,
  modelConfig,
  runtimeSettings,
  selectedProviderId,
  selectedExecutionProfile,
  reportArtifactEnabled,
  onProviderChange,
  onExecutionProfileChange,
  onReportArtifactToggle,
  onSaveModel,
  onSaveRuntimeSettings,
  saveStatus,
  runtimeSaveStatus,
}) {
  const browserTimezone =
    typeof Intl !== "undefined" ? Intl.DateTimeFormat().resolvedOptions().timeZone || "-" : "-";

  return (
    <div className="grid gap-4 xl:grid-cols-[1fr_360px]">
      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>设置</CardTitle>
          <CardDescription>放系统级配置，不再占用全局头部。</CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          <div className="border-b border-[var(--border)]/70 pb-4">
            <div className="text-sm font-medium text-[var(--foreground)]">运行方式</div>
            <div className="mt-1 text-xs text-[var(--muted-foreground)]">
              由 Node 设置决定任务编排方式；Python 只负责知识引擎和 JSON 结果。
            </div>
            <div className="mt-4 grid gap-4 md:grid-cols-[220px_1fr]">
              <div>
                <div className="mb-2 text-xs text-[var(--muted-foreground)]">任务编排模式</div>
                <Select value={selectedExecutionProfile} onChange={(event) => onExecutionProfileChange(event.target.value)}>
                  <option value="fast">快速模式 · 只跑核心数据表</option>
                  <option value="full">完整模式 · 额外生成报告</option>
                </Select>
              </div>
              <div>
                <div className="mb-2 text-xs text-[var(--muted-foreground)]">附加产物</div>
                <label className="flex items-center gap-2 text-sm text-[var(--foreground)]">
                  <input
                    type="checkbox"
                    checked={reportArtifactEnabled}
                    onChange={(event) => onReportArtifactToggle(event.target.checked)}
                  />
                  生成报告 JSON
                </label>
                <div className="mt-2 text-xs text-[var(--muted-foreground)]">
                  当前引擎模式：{runtimeSettings?.execution?.engine_mode || "llm_only"}。主链路只依赖大模型返回结构化结果，控制台与知识页展示由 Node 前端负责。
                </div>
              </div>
            </div>
            <div className="mt-4 flex items-center gap-3">
              <Button onClick={onSaveRuntimeSettings}>
                保存运行设置
              </Button>
              {runtimeSaveStatus ? (
                <div className={`text-sm ${runtimeSaveStatus.ok ? "text-[var(--primary)]" : "text-[var(--destructive)]"}`}>
                  {runtimeSaveStatus.message}
                </div>
              ) : null}
            </div>
          </div>

          <div className="border-b border-[var(--border)]/70 pb-4">
            <div className="text-sm font-medium text-[var(--foreground)]">模型切换</div>
            <div className="mt-1 text-xs text-[var(--muted-foreground)]">
              切换后将影响后续新任务的解析、建库和报告生成。
            </div>
            <div className="mt-4 flex flex-col gap-3 md:flex-row md:items-center">
              <Select value={selectedProviderId} onChange={(event) => onProviderChange(event.target.value)}>
                {(modelConfig?.providers || []).map((provider) => (
                  <option key={provider.id} value={provider.id}>
                    {provider.title} · {provider.model}
                  </option>
                ))}
              </Select>
              <Button onClick={onSaveModel} disabled={!selectedProviderId}>
                保存模型
              </Button>
            </div>
            {saveStatus ? (
              <div className={`mt-3 text-sm ${saveStatus.ok ? "text-[var(--primary)]" : "text-[var(--destructive)]"}`}>
                {saveStatus.message}
              </div>
            ) : null}
          </div>

          <div className="border-b border-[var(--border)]/70 pb-4">
            <div className="text-sm font-medium text-[var(--foreground)]">当前路由</div>
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              {Object.entries(modelConfig?.routing || {}).map(([route, providerId]) => {
                const provider = (modelConfig?.providers || []).find((item) => item.id === providerId);
                return (
                  <SummaryRow
                    key={route}
                    label={route}
                    value={provider ? `${provider.title}` : providerId}
                  />
                );
              })}
            </div>
          </div>

          <div>
            <div className="text-sm font-medium text-[var(--foreground)]">环境信息</div>
            <div className="mt-3 grid gap-3 md:grid-cols-2">
              <SummaryRow label="当前工作空间" value={workspaceId || "-"} />
              <SummaryRow label="浏览器时区" value={browserTimezone} />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>系统建议</CardTitle>
          <CardDescription>只放真正影响稳定性和可理解性的配置。</CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="border-b border-[var(--border)]/70 pb-3">
            <div className="text-sm font-medium text-[var(--foreground)]">模型策略</div>
            <div className="mt-2 text-xs text-[var(--muted-foreground)]">
              建库阶段建议使用更稳定、响应更快的模型；报告类模型不应阻塞主任务。
            </div>
          </div>
          <div className="border-b border-[var(--border)]/70 pb-3">
            <div className="text-sm font-medium text-[var(--foreground)]">时间显示</div>
            <div className="mt-2 text-xs text-[var(--muted-foreground)]">
              前端展示按本机浏览器时区显示；后端运行时间建议统一写入带时区时间戳。
            </div>
          </div>
          <div className="pb-1">
            <div className="text-sm font-medium text-[var(--foreground)]">任务策略</div>
            <div className="mt-2 text-xs text-[var(--muted-foreground)]">
              建议默认使用快速模式；附加产物只在确实需要时开启，避免录入一次就等待数分钟。
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

function AgentsView({
  agentGroups,
  skills,
  selectedAgentGroupId,
  selectedAgentGroup,
  agentGroupSpec,
  soulInput,
  enabledSkills,
  saveStatus,
  onSelectGroup,
  onSoulChange,
  onToggleSkill,
  onSave,
}) {
  return (
    <div className="grid gap-4 xl:grid-cols-[240px_1fr]">
      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>智能体</CardTitle>
          <CardDescription>一个功能域就是一个智能体，不再逐个暴露子智能体。</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-1">
            {agentGroups.length ? (
              agentGroups.map((group) => (
                <button
                  key={group.id}
                  className={`w-full border-b border-[var(--border)]/60 px-3 py-3 text-left transition ${
                    group.id === selectedAgentGroupId
                      ? "bg-[var(--accent)] text-[var(--foreground)]"
                      : "text-[var(--muted-foreground)] hover:bg-[var(--accent)] hover:text-[var(--foreground)]"
                  }`}
                  onClick={() => onSelectGroup(group.id)}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-medium">{group.label}</div>
                  </div>
                  <div className="mt-2 text-xs text-[var(--muted-foreground)]">{group.description}</div>
                </button>
              ))
            ) : (
              <div className="text-sm text-[var(--muted-foreground)]">没有可编辑的智能体。</div>
            )}
          </div>
        </CardContent>
      </Card>

      <Card className="bg-transparent">
        <CardHeader>
          <CardTitle>智能体设置</CardTitle>
          <CardDescription>只编辑这个智能体的 Soul 文档和共享 Skill，不再拆一堆子智能体提示词。</CardDescription>
        </CardHeader>
        <CardContent>
          {selectedAgentGroup ? (
            <div className="space-y-4">
              <div className="grid gap-3 md:grid-cols-3">
                <div className="border-b border-[var(--border)]/60 py-3">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">智能体</div>
                  <div className="mt-2 text-sm font-semibold text-[var(--foreground)]">{selectedAgentGroup.label}</div>
                </div>
                <div className="border-b border-[var(--border)]/60 py-3">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">智能体目录</div>
                  <div className="mt-2 text-sm font-semibold text-[var(--foreground)]">{agentGroupSpec?.agentDir || "-"}</div>
                </div>
                <div className="border-b border-[var(--border)]/60 py-3">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">Soul 文档</div>
                  <div className="mt-2 text-sm font-semibold text-[var(--foreground)]">{agentGroupSpec?.soulPath || "-"}</div>
                </div>
                <div className="border-b border-[var(--border)]/60 py-3 md:col-span-3">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">文档包</div>
                  <div className="mt-2 flex flex-wrap gap-2 text-xs text-[var(--muted-foreground)]">
                    {Object.entries(agentGroupSpec?.guideFiles || {}).map(([key, value]) => (
                      <Badge key={key} variant="outline">{String(value || key)}</Badge>
                    ))}
                  </div>
                </div>
                <div className="border-b border-[var(--border)]/60 py-3 md:col-span-3">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">运行方式</div>
                  <div className="mt-2 text-sm font-semibold text-[var(--foreground)]">单智能体入口</div>
                </div>
              </div>

              <div className="border-t border-[var(--border)]/70 pt-4 text-sm text-[var(--muted-foreground)]">{selectedAgentGroup.description}</div>

              <div className="border-t border-[var(--border)]/70 pt-4">
                <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">Skill 能力</div>
                <div className="mt-3 flex flex-wrap gap-2">
                  {skills.length ? (
                    skills.map((skill) => {
                      const active = enabledSkills.includes(skill.id);
                      return (
                        <button
                          key={skill.id}
                          className={`inline-flex items-center gap-2 rounded-lg border px-3 py-2 text-xs transition ${
                            active
                              ? "border-[var(--primary)] bg-[var(--accent)] text-[var(--foreground)]"
                              : "border-[var(--border)] bg-[var(--background)] text-[var(--muted-foreground)]"
                          }`}
                          onClick={() => onToggleSkill(skill.id)}
                        >
                          <span>{skill.title || skill.id}</span>
                          <Badge variant={active ? "default" : "outline"}>{active ? "已启用" : "未启用"}</Badge>
                        </button>
                      );
                    })
                  ) : (
                    <div className="text-sm text-[var(--muted-foreground)]">当前没有可用技能。</div>
                  )}
                </div>
                <div className="mt-3 text-xs text-[var(--muted-foreground)]">
                  启用后，这个智能体内部成员运行时都会共享这些 Skill。
                </div>
              </div>

              {agentGroupSpec ? (
                <>
              <div className="space-y-2">
                <div className="text-sm font-medium text-[var(--foreground)]">Soul 文档</div>
                <Textarea className="min-h-[320px] font-mono text-xs" value={soulInput} onChange={(event) => onSoulChange(event.target.value)} />
              </div>

              <div className="flex flex-wrap items-center gap-3">
                <Button onClick={onSave}>保存智能体</Button>
                {saveStatus ? (
                  <div className={`text-sm ${saveStatus.ok ? "text-[var(--primary)]" : "text-[var(--destructive)]"}`}>{saveStatus.message}</div>
                ) : null}
              </div>
                </>
              ) : (
                <div className="text-sm text-[var(--muted-foreground)]">未载入智能体配置。</div>
              )}
            </div>
          ) : (
            <div className="text-sm text-[var(--muted-foreground)]">未选择智能体分组。</div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function MetricCard({ label, value, hint, delta, icon: Icon }) {
  return (
    <Card className="rounded-[1.25rem]">
      <CardContent className="space-y-5 p-6">
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-center gap-2 text-sm font-medium text-[var(--muted-foreground)]">
            {Icon ? <Icon className="h-4 w-4" /> : null}
            {label}
          </div>
          {delta ? <Badge variant="outline">{delta}</Badge> : null}
        </div>
        <div className="text-4xl font-semibold tracking-tight">{value}</div>
        <div className="space-y-1">
          <div className="text-sm font-medium">{hint}</div>
          <div className="text-sm text-[var(--muted-foreground)]">持续观察该指标的变化趋势</div>
        </div>
      </CardContent>
    </Card>
  );
}

function NodeGraphView({ centerLabel, connections, onSelectConnection }) {
  const items = (connections || []).slice(0, 8);
  const radiusX = 36;
  const radiusY = 28;

  return (
    <div className="rounded-[calc(var(--radius)+0.08rem)] bg-[var(--background)] px-3 py-4">
      {items.length ? (
        <div className="relative h-[320px]">
          <svg className="absolute inset-0 h-full w-full" viewBox="0 0 100 100" preserveAspectRatio="none">
            {items.map((item, index) => {
              const angle = (Math.PI * 2 * index) / items.length - Math.PI / 2;
              const x = 50 + radiusX * Math.cos(angle);
              const y = 50 + radiusY * Math.sin(angle);
              return (
                <line
                  key={`edge-${item.targetId}-${index}`}
                  x1="50"
                  y1="50"
                  x2={x}
                  y2={y}
                  stroke="color-mix(in oklab, var(--primary) 35%, var(--border))"
                  strokeWidth="0.5"
                />
              );
            })}
          </svg>

          <div className="absolute left-1/2 top-1/2 z-10 w-[156px] -translate-x-1/2 -translate-y-1/2 rounded-[calc(var(--radius)+0.08rem)] bg-[var(--primary)] px-3 py-2 text-center text-xs font-medium text-[var(--primary-foreground)] shadow-sm">
            <div className="truncate">{centerLabel}</div>
            <div className="mt-1 text-[11px] opacity-85">当前节点</div>
          </div>

          {items.map((item, index) => {
            const angle = (Math.PI * 2 * index) / items.length - Math.PI / 2;
            const x = 50 + radiusX * Math.cos(angle);
            const y = 50 + radiusY * Math.sin(angle);
            return (
              <button
                key={`node-${item.targetId}-${index}`}
                className="absolute z-10 w-[136px] -translate-x-1/2 -translate-y-1/2 rounded-[calc(var(--radius)+0.05rem)] border border-[var(--border)]/60 bg-[var(--card)] px-2.5 py-2 text-left text-[11px] shadow-sm transition hover:bg-[var(--accent)]"
                style={{ left: `${x}%`, top: `${y}%` }}
                onClick={() => onSelectConnection(item)}
              >
                <div className="truncate font-medium text-[var(--foreground)]">{item.label}</div>
                <div className="mt-1 truncate text-[11px] text-[var(--muted-foreground)]">
                  {item.targetTableTitle || item.description}
                </div>
              </button>
            );
          })}
        </div>
      ) : (
        <div className="px-2 py-8 text-sm text-[var(--muted-foreground)]">当前节点没有可展示的连线关系。</div>
      )}
    </div>
  );
}

function TaskMetric({ label, value, task }) {
  return (
    <div className="border-b border-[var(--border)]/70 px-0 py-3">
      <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--muted-foreground)]">{label}</div>
      <div className="mt-2 flex flex-wrap items-center gap-2">
        <div className="text-[15px] font-semibold text-[var(--foreground)]">{value}</div>
        {task ? <MonitorBadge task={task} /> : null}
      </div>
    </div>
  );
}

function FailureMeta({ label, value }) {
  return (
    <div className="border-b border-[var(--destructive)]/18 px-0 py-3">
      <div className="text-[10px] uppercase tracking-[0.16em] text-[var(--destructive)]">{label}</div>
      <div className="mt-2 text-sm font-medium text-[var(--foreground)]">{value || "-"}</div>
    </div>
  );
}

function SummaryRow({ label, value }) {
  return (
    <div className="flex items-start justify-between gap-4 border-b border-[var(--border)]/70 px-0 py-3">
      <div className="text-xs text-[var(--muted-foreground)]">{label}</div>
      <div className="max-w-[60%] text-right text-sm font-medium text-[var(--foreground)]">{value}</div>
    </div>
  );
}

function StatusBadge({ task }) {
  const status = getEffectiveTaskStatus(task);
  const variant = ["failed", "blocked", "stale"].includes(status) ? "danger" : status === "running" ? "warm" : "default";
  return <Badge variant={variant}>{mapTaskStatus(status)}</Badge>;
}

function MonitorBadge({ task }) {
  const health = task?.monitor?.health;
  const effectiveStatus = getEffectiveTaskStatus(task);
  if (!health || health === effectiveStatus || health === "healthy" || health === "completed") {
    return null;
  }
  const variant = ["stale", "failed", "blocked", "degraded"].includes(health) ? "danger" : "outline";
  return <Badge variant={variant}>监视：{mapTaskStatus(health)}</Badge>;
}

function getEffectiveTaskStatus(task) {
  const baseStatus = task?.status || "unknown";
  const health = task?.monitor?.health;
  if (baseStatus === "running" && ["blocked", "failed"].includes(health)) {
    return health;
  }
  return baseStatus;
}

function mapTaskStatus(status) {
  const mapping = {
    running: "运行中",
    completed: "已完成",
    failed: "失败",
    blocked: "已阻塞",
    stale: "长时间无新进度",
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
  const groupLabel = mapAgentGroupLabel(name);
  if (groupLabel) return groupLabel;
  return AGENT_LABELS[name] || name;
}

function mapAgentGroupLabel(name) {
  const agentName = String(name || "").trim();
  if (!agentName) return "";
  if (["ontology_agent", "database_builder_agent"].includes(agentName)) {
    return "建库智能体";
  }
  if ([
    "parse_agent",
    "relation_agent",
    "dedup_agent",
    "schema_designer_agent",
    "placeholder_agent",
  ].includes(agentName)) {
    return "解析智能体";
  }
  if ([
    "claim_resolver_agent",
    "conflict_auditor_agent",
    "schema_engine",
    "memory_curator",
    "knowledge_store",
    "governance",
    "version_store",
  ].includes(agentName)) {
    return "治理智能体";
  }
  if ([
    "insight_agent",
    "report_agent",
    "wiki_builder_agent",
    "insight_generator",
    "dashboard_renderer",
  ].includes(agentName)) {
    return "输出智能体";
  }
  return "";
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

function summarizeTaskProgress(task, coreTimeline) {
  const stages = coreTimeline || [];
  const total = stages.length || 1;
  const completed = stages.filter((step) => step.status === "ok").length;
  const runningStep = stages.find((step) => step.status === "running");
  const failedStep = stages.find((step) => step.status === "failed");
  const activeLabel = failedStep
    ? `${failedStep.label || formatStepLabel(failedStep.action || "-")}未完成`
    : runningStep
      ? `${runningStep.label || formatStepLabel(runningStep.action || "-")}正在执行`
      : task?.status === "completed"
        ? "全部阶段已完成"
        : "等待下一步";
  const percent = task?.status === "completed"
    ? 100
    : Math.round((completed / total) * 100);

  return {
    total,
    completed,
    percent,
    label: activeLabel,
  };
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
  "entity_id",
  "event_id",
  "claim_id",
  "relation_id",
  "subject",
  "object",
  "predicate",
  "participants",
  "timestamp",
  "status",
  "primary_key",
]);

function getVisibleColumns(table) {
  const columns = table?.columns || [];
  if (table?.visibility === "system") return columns;
  const filtered = columns.filter((column) => !META_COLUMNS.has(column) && !/_id$/.test(column));
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
    summary: "摘要",
    alias: "别名",
    aliases: "别名",
    category: "分类",
    categories: "分类",
    tags: "标签",
    keyword: "关键词",
    keywords: "关键词",
    location: "地点",
    area: "区域",
    region: "地区",
    city: "城市",
    address: "地址",
    schedule: "时间",
    date: "日期",
    time: "时间",
    start_time: "开始时间",
    end_time: "结束时间",
    opening_hours: "开放时间",
    status: "状态",
    note: "备注",
    notes: "备注",
    content: "内容",
    content_type: "内容类型",
    text: "文本",
    source: "来源",
    source_name: "来源名称",
    phone: "电话",
    website: "链接",
    role: "角色",
    person: "人物",
    people: "人物",
    organization: "机构",
    venue: "地点",
    service: "服务",
    event: "事件",
    amount: "金额",
    price: "价格",
    count: "数量",
    level: "等级",
    priority: "优先级",
    result: "结果",
    decision: "结论",
    owner: "负责人",
    email: "邮箱",
    current_activity: "当前状态",
    physical_state: "身体状态",
    opinions: "看法",
    plans_to: "计划",
    size_perception: "规模印象",
    convenience: "便利性",
    has_food_delivery: "配送情况",
    available_at: "可获得地点",
    source_type: "资料类型",
    ingested_at: "录入时间",
    claim_text: "原句",
    relation_type: "关系类型",
    confidence: "可信度",
  };
  if (labels[column]) return labels[column];
  return String(column || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (value) => value.toUpperCase());
}

function formatTableTitle(table) {
  const raw = String(table?.title || table?.name || "").trim();
  const normalized = raw.toLowerCase();
  const labels = {
    area: "地区",
    areas: "地区",
    organization: "机构",
    organizations: "机构",
    service: "服务",
    services: "服务",
    venue: "地点",
    venues: "地点",
    event: "事件",
    events: "事件",
    person: "人物",
    persons: "人物",
    people: "人物",
    source: "来源",
    sources: "来源",
    company: "公司",
    companies: "公司",
    project: "项目",
    projects: "项目",
    task: "任务",
    tasks: "任务",
    product: "产品",
    products: "产品",
    relation: "关系",
    relations: "关系",
    claim: "事实",
    claims: "事实",
  };
  return labels[normalized] || raw || "未命名表";
}

function formatTableDescription(table) {
  if (table?.description) {
    return table.description;
  }
  const title = formatTableTitle(table);
  return `${title}的结构化记录视图`;
}

function buildReferenceMap(tables) {
  const map = new Map();
  for (const table of tables || []) {
    for (const row of table.rows || []) {
      for (const key of ["id", "entity_id", "event_id", "claim_id", "relation_id"]) {
        const value = row[key];
        if (!value) continue;
        map.set(value, row.name || row.title || row.description || value);
      }
    }
  }
  return map;
}

function formatDisplayValue(value, referenceMap) {
  if (Array.isArray(value)) {
    return value.map((item) => formatDisplayValue(item, referenceMap)).join(" / ");
  }
  if (value && typeof value === "object") {
    return JSON.stringify(value, null, 0);
  }
  if (typeof value === "string") {
    if (referenceMap?.has(value)) {
      return referenceMap.get(value);
    }
    if (/^(ent|evt|claim|rel)_[a-z0-9_]+$/i.test(value)) {
      return "内部引用";
    }
  }
  return stringifyValue(value);
}

function buildRowConnections(row, table, tables, referenceMap) {
  const connections = [];
  const rowValues = Object.entries(row || {});
  for (const [field, value] of rowValues) {
    const targets = Array.isArray(value) ? value : [value];
    for (const target of targets) {
      if (typeof target !== "string") continue;
      const matched = findTableRowById(tables, target);
      if (!matched) continue;
      connections.push({
        label: formatDisplayValue(target, referenceMap),
        description: `${formatColumnLabel(field)} → ${formatTableTitle(matched.table)}`,
        targetTableName: matched.table.name,
        targetTableTitle: formatTableTitle(matched.table),
        targetId: target,
      });
    }
  }
  return dedupeConnections(connections);
}

function buildWorkspaceEdges(nodes, tables, referenceMap) {
  const nodeIds = new Set(nodes.map((node) => node.id));
  const edges = [];
  for (const node of nodes) {
    const connections = buildRowConnections(node.row, node.table, tables, referenceMap);
    for (const connection of connections) {
      if (!nodeIds.has(connection.targetId)) continue;
      edges.push({
        sourceId: node.id,
        targetId: connection.targetId,
        label: connection.description,
      });
    }
  }
  return dedupeWorkspaceEdges(edges);
}

function dedupeWorkspaceEdges(edges) {
  const seen = new Set();
  return edges.filter((edge) => {
    const key = [edge.sourceId, edge.targetId].sort().join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function getNodeColor(tableName) {
  const palette = {
    areas: { accent: "#0284c7", soft: "rgba(2,132,199,0.10)", border: "rgba(2,132,199,0.28)", strong: "#075985" },
    area: { accent: "#0284c7", soft: "rgba(2,132,199,0.10)", border: "rgba(2,132,199,0.28)", strong: "#075985" },
    organizations: { accent: "#7c3aed", soft: "rgba(124,58,237,0.10)", border: "rgba(124,58,237,0.28)", strong: "#5b21b6" },
    organization: { accent: "#7c3aed", soft: "rgba(124,58,237,0.10)", border: "rgba(124,58,237,0.28)", strong: "#5b21b6" },
    services: { accent: "#ea580c", soft: "rgba(234,88,12,0.10)", border: "rgba(234,88,12,0.28)", strong: "#c2410c" },
    service: { accent: "#ea580c", soft: "rgba(234,88,12,0.10)", border: "rgba(234,88,12,0.28)", strong: "#c2410c" },
    venues: { accent: "#16a34a", soft: "rgba(22,163,74,0.10)", border: "rgba(22,163,74,0.28)", strong: "#166534" },
    venue: { accent: "#16a34a", soft: "rgba(22,163,74,0.10)", border: "rgba(22,163,74,0.28)", strong: "#166534" },
    events: { accent: "#dc2626", soft: "rgba(220,38,38,0.10)", border: "rgba(220,38,38,0.28)", strong: "#991b1b" },
    event: { accent: "#dc2626", soft: "rgba(220,38,38,0.10)", border: "rgba(220,38,38,0.28)", strong: "#991b1b" },
    people: { accent: "#d97706", soft: "rgba(217,119,6,0.10)", border: "rgba(217,119,6,0.28)", strong: "#92400e" },
    person: { accent: "#d97706", soft: "rgba(217,119,6,0.10)", border: "rgba(217,119,6,0.28)", strong: "#92400e" },
    persons: { accent: "#d97706", soft: "rgba(217,119,6,0.10)", border: "rgba(217,119,6,0.28)", strong: "#92400e" },
    sources: { accent: "#475569", soft: "rgba(71,85,105,0.10)", border: "rgba(71,85,105,0.28)", strong: "#334155" },
    source: { accent: "#475569", soft: "rgba(71,85,105,0.10)", border: "rgba(71,85,105,0.28)", strong: "#334155" },
  };
  return palette[String(tableName || "").toLowerCase()] || {
    accent: "#64748b",
    soft: "rgba(100,116,139,0.10)",
    border: "rgba(100,116,139,0.24)",
    strong: "#334155",
  };
}

function findTableRowById(tables, value) {
  for (const table of tables || []) {
    for (const row of table.rows || []) {
      const candidates = [row.id, row.entity_id, row.event_id, row.claim_id, row.relation_id].filter(Boolean);
      if (candidates.includes(value)) {
        return { table, row };
      }
    }
  }
  return null;
}

function dedupeConnections(connections) {
  const seen = new Set();
  return connections.filter((item) => {
    const key = `${item.label}|${item.description}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function buildRowSourceSnippets(row, sources) {
  const refs = [];
  if (row?.source_ref) refs.push(row.source_ref);
  if (Array.isArray(row?.source_refs)) refs.push(...row.source_refs);
  const uniqueRefs = Array.from(new Set(refs.filter(Boolean)));
  return uniqueRefs
    .map((ref) => {
      const match = (sources || []).find((item) => item.source_id === ref);
      if (!match) return null;
      return {
        title: match.metadata?.filename || match.source_id || "资料来源",
        meta: [match.source_type || "doc", match.ingested_at || ""].filter(Boolean).join(" · "),
        summary: buildSourceSummary(match.content || "", match.context_hints?.note || ""),
      };
    })
    .filter(Boolean);
}

function buildSourceSummary(content, note = "") {
  const normalized = String(content || "").replace(/\s+/g, " ").trim();
  const summary = normalized.length > 120 ? `${normalized.slice(0, 120)}...` : normalized || "无内容";
  return note ? `${summary} · 备注：${note}` : summary;
}

function parseTaskDate(value) {
  if (!value) return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  const normalized = String(value).trim();
  if (!normalized) return null;
  const hasTimezone = /(?:Z|[+-]\d{2}:\d{2})$/.test(normalized);
  const utcLike = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$/;
  const parsed = new Date(hasTimezone || !utcLike.test(normalized) ? normalized : `${normalized}Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function formatTaskTime(value) {
  if (!value) return "-";
  const date = parseTaskDate(value);
  if (!date) return value;
  return date.toLocaleString("zh-CN", { hour12: false });
}

function formatDuration(startValue, endValue) {
  const start = parseTaskDate(startValue);
  const end = parseTaskDate(endValue);
  if (!start || !end || end < start) return "-";
  const totalSeconds = Math.floor((end.getTime() - start.getTime()) / 1000);
  if (totalSeconds < 60) return `${totalSeconds} 秒`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes} 分 ${seconds} 秒`;
}

function formatElapsed(startValue) {
  const start = parseTaskDate(startValue);
  if (!start) return "-";
  const totalSeconds = Math.max(0, Math.floor((Date.now() - start.getTime()) / 1000));
  if (totalSeconds < 60) return `${totalSeconds} 秒`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return `${minutes} 分 ${seconds} 秒`;
  const hours = Math.floor(minutes / 60);
  const remainMinutes = minutes % 60;
  return `${hours} 小时 ${remainMinutes} 分`;
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

function hasTaskFailure(task) {
  return ["failed", "blocked"].includes(task?.status);
}

function findFailedStep(task) {
  const entries = task?.stepEntries || task?.recentSteps || [];
  return [...entries].reverse().find((step) => ["failed", "blocked"].includes(step.status)) || null;
}

function findLiveStep(task) {
  const entries = condenseStepEntries(task?.stepEntries || task?.recentSteps || []);
  if (!entries.length) return null;
  const currentAction = task?.current_step;
  const exact = [...entries].reverse().find((step) => step.action === currentAction && step.status === "running");
  if (exact) return exact;
  return [...entries].reverse().find((step) => step.status === "running") || null;
}

function formatTaskFailureTitle(task) {
  const failedStep = findFailedStep(task);
  const stepLabel = formatStepLabel(task?.current_step || failedStep?.action || "pipeline");
  return `${stepLabel} 未完成`;
}

function formatTaskFailureMessage(task) {
  const failedStep = findFailedStep(task);
  const raw = task?.resume_hint || failedStep?.error || "";
  if (!raw) return "任务中断，但当前没有记录到更详细的错误说明。";
  const simplified = raw
    .replace(/^[a-zA-Z0-9_]+ failed:\s*/i, "")
    .replace(/^HTTP Error\s*/i, "HTTP ")
    .replace(/\bBad Gateway\b/i, "网关错误")
    .replace(/\bBad Request\b/i, "请求不合法")
    .replace(/\bService Unavailable\b/i, "服务不可用");
  return simplified;
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
