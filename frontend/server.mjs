import { createServer } from "node:http";
import { readFile, readdir, stat, writeFile, mkdir, mkdtemp, cp } from "node:fs/promises";
import { createReadStream, existsSync } from "node:fs";
import { rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Busboy from "busboy";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(__dirname, "dist");
const publicDir = existsSync(distDir) ? distDir : path.join(__dirname, "public");
const workspacesDir = path.join(rootDir, "output", "workspaces");
const agentsDir = path.join(rootDir, "mindvault", "agents");
const agentGroupsConfigPath = path.join(rootDir, "config", "agent_groups.json");
const modelConfigPath = path.join(rootDir, "config", "model_config.json");
const runtimeConfigPath = path.join(rootDir, "config", "runtime_config.json");
const skillsDir = path.join(rootDir, "skills");
const skillsRegistryPath = path.join(rootDir, "config", "skills_registry.json");
const agentSkillBindingsPath = path.join(rootDir, "config", "agent_skill_bindings.json");
const port = Number(process.env.PORT || 4310);
const host = process.env.HOST || "127.0.0.1";

const mimeTypes = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

const server = createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host}`);

  if (url.pathname === "/health") {
    return sendJson(res, {
      ok: true,
      service: "mindvault-frontend",
      time: new Date().toISOString(),
    });
  }

  if (url.pathname === "/api/workspaces") {
    if (req.method === "GET") {
      return sendJson(res, await listWorkspaces());
    }
    if (req.method === "POST") {
      return handleWorkspaceCreate(req, res);
    }
  }

  if (url.pathname === "/api/agents" && req.method === "GET") {
    return sendJson(res, await listAgentSpecs());
  }

  if (url.pathname === "/api/agent-groups" && req.method === "GET") {
    return sendJson(res, await listAgentGroupSpecs());
  }

  if (url.pathname === "/api/skills") {
    if (req.method === "GET") {
      return sendJson(res, await listSkills());
    }
    if (req.method === "POST") {
      return handleSkillInstall(req, res);
    }
  }

  if (url.pathname.startsWith("/api/skills/")) {
    const suffix = decodeURIComponent(url.pathname.replace("/api/skills/", "").replace(/\/$/, ""));
    if (suffix.endsWith("/sync") && req.method === "POST") {
      const skillId = suffix.replace(/\/sync$/, "");
      return handleSkillSync(res, skillId);
    }
    if (req.method === "PUT") {
      return handleSkillUpdate(req, res, suffix);
    }
  }

  if (url.pathname === "/api/models") {
    if (req.method === "GET") {
      return sendJson(res, await readModelConfig());
    }
    if (req.method === "PUT") {
      return handleModelConfigUpdate(req, res);
    }
  }

  if (url.pathname === "/api/runtime-settings") {
    if (req.method === "GET") {
      return sendJson(res, await readRuntimeConfig());
    }
    if (req.method === "PUT") {
      return handleRuntimeConfigUpdate(req, res);
    }
  }

  if (url.pathname.startsWith("/api/agents/")) {
    const agentName = decodeURIComponent(url.pathname.replace("/api/agents/", "").replace(/\/$/, ""));
    if (req.method === "GET") {
      return sendJson(res, await readAgentSpec(agentName), 200);
    }
    if (req.method === "PUT") {
      return handleAgentUpdate(req, res, agentName);
    }
  }

  if (url.pathname.startsWith("/api/agent-groups/")) {
    const groupId = decodeURIComponent(url.pathname.replace("/api/agent-groups/", "").replace(/\/$/, ""));
    if (req.method === "GET") {
      return sendJson(res, await readAgentGroupSpec(groupId), 200);
    }
    if (req.method === "PUT") {
      return handleAgentGroupUpdate(req, res, groupId);
    }
  }

  if (url.pathname.startsWith("/api/workspaces/") && url.pathname.endsWith("/ingest") && req.method === "POST") {
    const workspaceId = decodeURIComponent(
      url.pathname.replace("/api/workspaces/", "").replace("/ingest", "").replace(/\/$/, ""),
    );
    return handleIngest(req, res, workspaceId);
  }

  if (url.pathname.startsWith("/api/workspaces/") && url.pathname.includes("/tasks/") && req.method === "DELETE") {
    const parts = url.pathname.split("/").filter(Boolean);
    const workspaceId = decodeURIComponent(parts[2] || "");
    const taskId = decodeURIComponent(parts[4] || "");
    return handleTaskDelete(res, workspaceId, taskId);
  }

  if (url.pathname.startsWith("/api/workspaces/")) {
    const workspaceId = decodeURIComponent(url.pathname.replace("/api/workspaces/", "").replace(/\/$/, ""));
    if (req.method === "DELETE") {
      return handleWorkspaceDelete(res, workspaceId);
    }
    return sendJson(res, await readWorkspacePayload(workspaceId), 200);
  }

  return serveStatic(url.pathname, res);
});

server.listen(port, host, () => {
  console.log(`MindVault frontend: http://${host}:${port}`);
  void maybeAutoUpdateSkills();
});

async function listWorkspaces() {
  if (!existsSync(workspacesDir)) {
    return { workspaces: [] };
  }

  const names = await readdir(workspacesDir);
  const workspaces = [];
  for (const name of names.sort()) {
    const absolute = path.join(workspacesDir, name);
    const meta = await safeStat(absolute);
    if (!meta?.isDirectory()) continue;

    const multiDbPath = path.join(absolute, "multi_db", "multi_db.json");
    const planPath = path.join(absolute, "multi_db", "database_plan.json");
    const hasMultiDb = existsSync(multiDbPath);
    const hasPlan = existsSync(planPath);
    workspaces.push({
      id: name,
      hasMultiDb,
      hasPlan,
      updatedAt: meta.mtime.toISOString(),
    });
  }

  return { workspaces };
}

async function handleWorkspaceCreate(req, res) {
  try {
    const body = await readJsonBody(req);
    const workspaceId = sanitizeWorkspaceId(body?.workspaceId || "");
    if (!workspaceId) {
      return sendJson(res, { error: "workspaceId is required" }, 400);
    }

    const workspaceRoot = path.join(workspacesDir, workspaceId);
    if (existsSync(workspaceRoot)) {
      return sendJson(res, { error: "workspace already exists" }, 409);
    }

    const dirs = [
      workspaceRoot,
      path.join(workspaceRoot, "raw"),
      path.join(workspaceRoot, "extracted"),
      path.join(workspaceRoot, "canonical"),
      path.join(workspaceRoot, "snapshots"),
      path.join(workspaceRoot, "reports"),
      path.join(workspaceRoot, "visuals"),
      path.join(workspaceRoot, "governance"),
      path.join(workspaceRoot, "config"),
      path.join(workspaceRoot, "wiki"),
      path.join(workspaceRoot, "tasks"),
      path.join(workspaceRoot, "multi_db"),
    ];
    for (const dir of dirs) {
      await mkdir(dir, { recursive: true });
    }

    await writeFile(path.join(workspaceRoot, "raw", "sources.json"), "[]\n", "utf-8");
    await writeFile(path.join(workspaceRoot, "agent_trace.json"), "[]\n", "utf-8");
    await writeFile(
      path.join(workspaceRoot, "multi_db", "database_plan.json"),
      JSON.stringify({ domain: "", databases: [], relations: [] }, null, 2),
      "utf-8",
    );
    await writeFile(
      path.join(workspaceRoot, "multi_db", "multi_db.json"),
      JSON.stringify({ domain: "", databases: [], relations: [] }, null, 2),
      "utf-8",
    );

    return sendJson(res, {
      success: true,
      workspace: workspaceId,
      message: "工作空间已创建。",
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function handleWorkspaceDelete(res, workspaceId) {
  try {
    const safeId = sanitizeWorkspaceId(workspaceId);
    const workspaceRoot = path.join(workspacesDir, safeId);
    if (!existsSync(workspaceRoot)) {
      return sendJson(res, { error: "workspace not found" }, 404);
    }
    await rm(workspaceRoot, { recursive: true, force: true });
    return sendJson(res, {
      success: true,
      workspace: safeId,
      message: "工作空间已删除。",
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function handleTaskDelete(res, workspaceId, taskId) {
  try {
    const safeWorkspaceId = sanitizeWorkspaceId(workspaceId);
    const safeTaskId = String(taskId || "").trim().replace(/[^a-zA-Z0-9_-]/g, "_");
    if (!safeWorkspaceId || !safeTaskId) {
      return sendJson(res, { error: "invalid workspace or task id" }, 400);
    }
    const taskRoot = path.join(workspacesDir, safeWorkspaceId, "tasks", safeTaskId);
    const meta = await safeStat(taskRoot);
    if (!meta?.isDirectory()) {
      return sendJson(res, { error: "task not found" }, 404);
    }
    await rm(taskRoot, { recursive: true, force: true });
    return sendJson(res, {
      success: true,
      workspace: safeWorkspaceId,
      task_id: safeTaskId,
      message: "任务已删除。",
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function listAgentSpecs() {
  if (!existsSync(agentsDir)) return { agents: [] };
  const entries = await readdir(agentsDir);
  const agents = [];
  for (const entry of entries.sort()) {
    if (!entry.endsWith(".yaml")) continue;
    const spec = await readAgentSpec(path.basename(entry, ".yaml"));
    if (!spec?.name) continue;
    agents.push({
      name: spec.name,
      role: spec.role,
      configPath: spec.configPath,
      promptPath: spec.promptPath,
      hasPrompt: Boolean(spec.promptPath),
    });
  }
  return { agents };
}

async function readAgentGroupsConfig() {
  return readJsonSafe(agentGroupsConfigPath, { groups: [] });
}

async function listAgentGroupSpecs() {
  const config = await readAgentGroupsConfig();
  const groups = [];
  for (const group of config.groups || []) {
    const spec = await readAgentGroupSpec(group.id);
    if (!spec?.id) continue;
    groups.push({
      id: spec.id,
      label: spec.label,
      description: spec.description,
      soulPath: spec.soulPath,
    });
  }
  return { groups };
}

async function readAgentGroupSpec(groupId) {
  const config = await readAgentGroupsConfig();
  const group = (config.groups || []).find((item) => item.id === groupId);
  if (!group) {
    return { error: `agent group not found: ${groupId}` };
  }
  const soulPath = path.join(rootDir, group.soul_path || "");
  const soulContent = existsSync(soulPath) ? await readFile(soulPath, "utf-8") : "";
  const bindings = await readAgentSkillBindings();
  const firstAgent = (group.internal_agents || [])[0];
  return {
    id: group.id,
    label: group.label,
    description: group.description,
    soulPath: group.soul_path || "",
    soulContent,
    enabledSkills: firstAgent ? bindings.agents?.[firstAgent] || [] : [],
  };
}

async function readAgentSpec(agentName) {
  const configPath = path.join(agentsDir, `${agentName}.yaml`);
  if (!existsSync(configPath)) {
    return { error: `agent not found: ${agentName}` };
  }
  const configContent = await readFile(configPath, "utf-8");
  const promptRelativePath = extractPromptTemplatePath(configContent);
  const promptPath = promptRelativePath ? path.join(rootDir, promptRelativePath) : "";
  const promptContent = promptPath && existsSync(promptPath) ? await readFile(promptPath, "utf-8") : "";
  const bindings = await readAgentSkillBindings();
  return {
    name: agentName,
    role: extractRole(configContent),
    configPath: toWorkspaceRelative(configPath),
    promptPath: promptPath ? toWorkspaceRelative(promptPath) : "",
    configContent,
    promptContent,
    enabledSkills: bindings.agents?.[agentName] || [],
  };
}

async function readModelConfig() {
  const config = await readJsonSafe(modelConfigPath, { providers: {}, routing: {} });
  const providers = Object.entries(config.providers || {}).map(([id, provider]) => ({
    id,
    title: provider.title || provider.model || id,
    base_url: provider.base_url || "",
    model: provider.model || "",
    timeout_seconds: Number(provider.timeout_seconds || 120),
    max_retries: Number(provider.max_retries || 2),
  }));
  const routing = config.routing || {};
  const currentProviderId = routing.parse || "";
  return {
    providers,
    routing,
    currentProviderId,
    currentProvider: providers.find((provider) => provider.id === currentProviderId) || null,
  };
}

async function handleModelConfigUpdate(req, res) {
  try {
    const body = await readJsonBody(req);
    const providerId = String(body?.providerId || "").trim();
    const config = await readJsonSafe(modelConfigPath, { providers: {}, routing: {} });
    if (!providerId || !config.providers?.[providerId]) {
      return sendJson(res, { error: "provider not found" }, 404);
    }

    config.routing = {
      ...(config.routing || {}),
      parse: providerId,
      insight: providerId,
      report: providerId,
    };

    await writeFile(modelConfigPath, JSON.stringify(config, null, 2), "utf-8");
    return sendJson(res, {
      success: true,
      message: "默认模型已切换。",
      ...(await readModelConfig()),
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function readRuntimeConfig() {
  const config = await readJsonSafe(runtimeConfigPath, {
    execution: { profile: "fast", engine_mode: "json_engine" },
    artifacts: { report: false },
  });
  return {
    execution: {
      profile: ["fast", "full"].includes(config?.execution?.profile) ? config.execution.profile : "fast",
      engine_mode: config?.execution?.engine_mode || "json_engine",
    },
    artifacts: {
      report: Boolean(config?.artifacts?.report),
    },
  };
}

async function handleRuntimeConfigUpdate(req, res) {
  try {
    const body = await readJsonBody(req);
    const current = await readRuntimeConfig();
    const next = {
      execution: {
        profile: ["fast", "full"].includes(body?.execution?.profile) ? body.execution.profile : current.execution.profile,
        engine_mode: "json_engine",
      },
      artifacts: {
        report: typeof body?.artifacts?.report === "boolean" ? body.artifacts.report : current.artifacts.report,
      },
    };
    await writeFile(runtimeConfigPath, JSON.stringify(next, null, 2), "utf-8");
    return sendJson(res, {
      success: true,
      message: "运行设置已保存。",
      ...(await readRuntimeConfig()),
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function handleAgentUpdate(req, res, agentName) {
  try {
    const body = await readJsonBody(req);
    const configPath = path.join(agentsDir, `${agentName}.yaml`);
    if (!existsSync(configPath)) {
      return sendJson(res, { error: `agent not found: ${agentName}` }, 404);
    }
    if (typeof body.configContent === "string") {
      await writeFile(configPath, body.configContent, "utf-8");
    }
    const nextConfigContent = await readFile(configPath, "utf-8");
    const promptRelativePath = extractPromptTemplatePath(nextConfigContent);
    const promptPath = promptRelativePath ? path.join(rootDir, promptRelativePath) : "";
    if (promptPath && typeof body.promptContent === "string") {
      await mkdir(path.dirname(promptPath), { recursive: true });
      await writeFile(promptPath, body.promptContent, "utf-8");
    }
    if (Array.isArray(body.enabledSkills)) {
      const bindings = await readAgentSkillBindings();
      bindings.agents = {
        ...(bindings.agents || {}),
        [agentName]: body.enabledSkills.filter(Boolean),
      };
      await writeAgentSkillBindings(bindings);
    }
    return sendJson(res, {
      success: true,
      agent: await readAgentSpec(agentName),
      message: "智能体提示词已保存。",
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function handleAgentGroupUpdate(req, res, groupId) {
  try {
    const body = await readJsonBody(req);
    const config = await readAgentGroupsConfig();
    const group = (config.groups || []).find((item) => item.id === groupId);
    if (!group) {
      return sendJson(res, { error: `agent group not found: ${groupId}` }, 404);
    }
    const soulPath = path.join(rootDir, group.soul_path || "");
    if (typeof body.soulContent === "string" && soulPath) {
      await mkdir(path.dirname(soulPath), { recursive: true });
      await writeFile(soulPath, body.soulContent, "utf-8");
    }
    if (Array.isArray(body.enabledSkills)) {
      const bindings = await readAgentSkillBindings();
      const nextSkills = body.enabledSkills.filter(Boolean);
      bindings.agents = {
        ...(bindings.agents || {}),
      };
      for (const agentName of group.internal_agents || []) {
        bindings.agents[agentName] = nextSkills;
      }
      await writeAgentSkillBindings(bindings);
    }
    return sendJson(res, {
      success: true,
      message: "智能体配置已保存。",
      group: await readAgentGroupSpec(groupId),
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function readAgentSkillBindings() {
  return readJsonSafe(agentSkillBindingsPath, { agents: {} });
}

async function writeAgentSkillBindings(payload) {
  await writeFile(agentSkillBindingsPath, JSON.stringify(payload, null, 2), "utf-8");
}

async function listSkills() {
  const registry = await readSkillsRegistry();
  const skills = await Promise.all(
    (registry.skills || []).map(async (entry) => enrichSkillEntry(entry)),
  );
  return { skills };
}

async function handleSkillInstall(req, res) {
  try {
    const body = await readJsonBody(req);
    const sourceUrl = String(body?.sourceUrl || "").trim();
    if (!sourceUrl) {
      return sendJson(res, { error: "sourceUrl is required" }, 400);
    }
    const installResult = await installSkillFromGithubUrl(sourceUrl);
    const registry = await readSkillsRegistry();
    const existingIndex = (registry.skills || []).findIndex((skill) => skill.id === installResult.id);
    const nextEntry = {
      id: installResult.id,
      title: installResult.title,
      path: installResult.path,
      source: {
        type: "github",
        url: sourceUrl,
      },
      auto_update: body?.autoUpdate !== false,
      status: "installed",
      last_updated_at: new Date().toISOString(),
      last_checked_at: new Date().toISOString(),
    };
    if (existingIndex >= 0) {
      registry.skills[existingIndex] = { ...registry.skills[existingIndex], ...nextEntry };
    } else {
      registry.skills = [...(registry.skills || []), nextEntry];
    }
    await writeSkillsRegistry(registry);
    return sendJson(res, { success: true, message: "技能已安装。", skill: await enrichSkillEntry(nextEntry) });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function handleSkillUpdate(req, res, skillId) {
  try {
    const body = await readJsonBody(req);
    const registry = await readSkillsRegistry();
    const index = (registry.skills || []).findIndex((skill) => skill.id === skillId);
    if (index < 0) {
      return sendJson(res, { error: "skill not found" }, 404);
    }
    registry.skills[index] = {
      ...registry.skills[index],
      auto_update: body?.auto_update ?? registry.skills[index].auto_update ?? false,
    };
    await writeSkillsRegistry(registry);
    return sendJson(res, { success: true, skill: await enrichSkillEntry(registry.skills[index]) });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function handleSkillSync(res, skillId) {
  try {
    const result = await syncSkillById(skillId);
    return sendJson(res, { success: true, ...result });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

async function readWorkspacePayload(workspaceId) {
  const workspaceRoot = path.join(workspacesDir, workspaceId);
  const multiDbPath = path.join(workspaceRoot, "multi_db", "multi_db.json");
  const planPath = path.join(workspaceRoot, "multi_db", "database_plan.json");
  const tracePath = path.join(workspaceRoot, "agent_trace.json");
  const rawSourcesPath = path.join(workspaceRoot, "raw", "sources.json");

  if (!existsSync(workspaceRoot)) {
    return { error: `Workspace not found: ${workspaceId}` };
  }

  const [multiDb, plan, trace, rawSources, tasks] = await Promise.all([
    readJsonSafe(multiDbPath, { databases: [], relations: [] }),
    readJsonSafe(planPath, { databases: [], relations: [] }),
    readJsonSafe(tracePath, []),
    readJsonSafe(rawSourcesPath, []),
    readTasks(path.join(workspaceRoot, "tasks")),
  ]);
  const normalizedPlan = applyDatabaseVisibility(plan);
  const normalizedMultiDb = applyDatabaseVisibility(multiDb, normalizedPlan);
  const recentSources = Array.isArray(rawSources) ? rawSources.slice(-8).reverse() : [];
  const tasksWithImpact = tasks.map((task) => ({
    ...task,
    impact: summarizeTaskImpact(task, rawSources, normalizedMultiDb),
  }));
  const latestTask = tasksWithImpact[0] || null;

  return {
    workspace: workspaceId,
    tables: normalizedMultiDb.databases || [],
    multiDb: normalizedMultiDb,
    databasePlan: normalizedPlan,
    trace,
    tasks: tasksWithImpact,
    latestTask,
    recentSources,
  };
}

async function readSkillsRegistry() {
  const registry = await readJsonSafe(skillsRegistryPath, { skills: [] });
  return {
    skills: Array.isArray(registry.skills) ? registry.skills : [],
  };
}

async function writeSkillsRegistry(registry) {
  await writeFile(skillsRegistryPath, JSON.stringify(registry, null, 2), "utf-8");
}

async function enrichSkillEntry(entry) {
  const skillPath = path.join(rootDir, entry.path || "");
  const exists = existsSync(skillPath);
  const manifestPath = path.join(skillPath, "SKILL.md");
  const manifest = existsSync(manifestPath) ? await readFile(manifestPath, "utf-8") : "";
  const meta = extractSkillFrontmatter(manifest);
  return {
    ...entry,
    title: entry.title || meta.name || entry.id,
    description: meta.description || "",
    exists,
    manifestPath: exists ? toWorkspaceRelative(manifestPath) : "",
  };
}

function extractSkillFrontmatter(content) {
  const match = String(content || "").match(/^---\n([\s\S]*?)\n---/);
  if (!match) return { name: "", description: "" };
  const yaml = match[1];
  const name = (yaml.match(/^name:\s*(.+)$/m)?.[1] || "").trim();
  const description = (yaml.match(/^description:\s*(.+)$/m)?.[1] || "").trim();
  return { name, description };
}

async function installSkillFromGithubUrl(sourceUrl) {
  await mkdir(skillsDir, { recursive: true });
  await runInstaller(sourceUrl, skillsDir);
  const parsed = parseSkillGithubUrl(sourceUrl);
  const skillId = path.basename(parsed.skillPath);
  const skillPath = path.join(skillsDir, skillId);
  const manifest = existsSync(path.join(skillPath, "SKILL.md"))
    ? await readFile(path.join(skillPath, "SKILL.md"), "utf-8")
    : "";
  const meta = extractSkillFrontmatter(manifest);
  return {
    id: skillId,
    title: meta.name || skillId,
    path: toWorkspaceRelative(skillPath),
  };
}

function parseSkillGithubUrl(sourceUrl) {
  const match = String(sourceUrl).match(/^https:\/\/github\.com\/([^/]+)\/([^/]+)\/tree\/([^/]+)\/(.+)$/);
  if (!match) {
    throw new Error("暂只支持 GitHub tree URL。");
  }
  return {
    owner: match[1],
    repo: match[2],
    ref: match[3],
    skillPath: match[4],
  };
}

function runInstaller(sourceUrl, destDir) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      "python3",
      [
        "/Users/a1/.codex/skills/.system/skill-installer/scripts/install-skill-from-github.py",
        "--url",
        sourceUrl,
        "--dest",
        destDir,
      ],
      { cwd: rootDir },
    );
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf-8");
    });
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(stderr.trim() || `installer exited with ${code}`));
      }
    });
    child.on("error", reject);
  });
}

async function syncSkillById(skillId) {
  const registry = await readSkillsRegistry();
  const index = (registry.skills || []).findIndex((skill) => skill.id === skillId);
  if (index < 0) {
    throw new Error("skill not found");
  }
  const entry = registry.skills[index];
  if (entry.source?.type !== "github" || !entry.source?.url) {
    throw new Error("skill source is not configured for sync");
  }
  const tempRoot = await mkdtemp(path.join(tmpdir(), "mindvault-skill-sync-"));
  await runInstaller(entry.source.url, tempRoot);
  const parsed = parseSkillGithubUrl(entry.source.url);
  const nextSkillDir = path.join(tempRoot, path.basename(parsed.skillPath));
  const targetSkillDir = path.join(rootDir, entry.path);
  if (!existsSync(nextSkillDir)) {
    throw new Error("synced skill folder not found");
  }
  await rm(targetSkillDir, { recursive: true, force: true });
  await mkdir(path.dirname(targetSkillDir), { recursive: true });
  await cp(nextSkillDir, targetSkillDir, { recursive: true });
  registry.skills[index] = {
    ...entry,
    status: "installed",
    last_checked_at: new Date().toISOString(),
    last_updated_at: new Date().toISOString(),
    last_error: "",
  };
  await writeSkillsRegistry(registry);
  return {
    message: "技能已同步到最新版本。",
    skill: await enrichSkillEntry(registry.skills[index]),
  };
}

async function maybeAutoUpdateSkills() {
  const registry = await readSkillsRegistry();
  for (const entry of registry.skills || []) {
    if (!entry.auto_update) continue;
    const lastChecked = Date.parse(entry.last_checked_at || "");
    const due = Number.isNaN(lastChecked) || Date.now() - lastChecked > 1000 * 60 * 60 * 12;
    if (!due) continue;
    try {
      await syncSkillById(entry.id);
    } catch (error) {
      const latest = await readSkillsRegistry();
      const index = (latest.skills || []).findIndex((skill) => skill.id === entry.id);
      if (index >= 0) {
        latest.skills[index] = {
          ...latest.skills[index],
          last_checked_at: new Date().toISOString(),
          last_error: error.message,
        };
        await writeSkillsRegistry(latest);
      }
    }
  }
}

async function readTasks(taskDir) {
  if (!existsSync(taskDir)) return [];
  const names = (await readdir(taskDir)).sort().reverse();
  const tasks = [];
  for (const name of names) {
    const absolute = path.join(taskDir, name);
    const meta = await safeStat(absolute);
    if (!meta?.isDirectory()) continue;
    const taskPath = path.join(absolute, "task.json");
    const task = await readJsonSafe(taskPath, null);
    const stepLogPath = path.join(absolute, "step_log.jsonl");
    const stepEntries = await readStepLog(stepLogPath, 200);
    const recentSteps = stepEntries.slice(-12);
    if (task) {
      tasks.push({
        ...task,
        recentSteps,
        stepEntries,
        stepTimeline: summarizeStepTimeline(stepEntries),
        monitor: summarizeTask(task, stepEntries),
      });
    }
  }
  return tasks;
}

async function readStepLog(stepLogPath, maxItems) {
  if (!existsSync(stepLogPath)) return [];
  try {
    const raw = await readFile(stepLogPath, "utf-8");
    return raw
      .trim()
      .split("\n")
      .filter(Boolean)
      .slice(-maxItems)
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

async function serveStatic(urlPath, res) {
  const normalized = urlPath === "/" ? "/index.html" : urlPath;
  const filePath = path.join(publicDir, normalized);
  if (!filePath.startsWith(publicDir)) {
    return sendText(res, "Forbidden", 403);
  }

  if (!existsSync(filePath)) {
    if (existsSync(path.join(publicDir, "index.html"))) {
      return streamFile(path.join(publicDir, "index.html"), res);
    }
    return sendText(res, "Not found", 404);
  }

  return streamFile(filePath, res);
}

async function readJsonSafe(filePath, fallback) {
  if (!existsSync(filePath)) return fallback;
  try {
    const raw = await readFile(filePath, "utf-8");
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function safeStat(target) {
  try {
    return await stat(target);
  } catch {
    return null;
  }
}

function sendJson(res, payload, status = 200) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store, max-age=0",
  });
  res.end(JSON.stringify(payload));
}

function sendText(res, text, status = 200) {
  res.writeHead(status, {
    "Content-Type": "text/plain; charset=utf-8",
    "Cache-Control": "no-store, max-age=0",
  });
  res.end(text);
}

function streamFile(filePath, res) {
  const ext = path.extname(filePath);
  res.writeHead(200, {
    "Content-Type": mimeTypes[ext] || "application/octet-stream",
    "Cache-Control": "no-store, max-age=0",
  });
  createReadStream(filePath).pipe(res);
}

function summarizeTask(task, recentSteps) {
  const heartbeatAgeSeconds = getHeartbeatAgeSeconds(task.last_heartbeat);
  const activityAgeSeconds = getActivityAgeSeconds(task, recentSteps);
  const isStale = task.status === "running" && activityAgeSeconds !== null && activityAgeSeconds > 300;
  const recentFallbacks = recentSteps.filter((step) => step.status === "fallback").length;
  const recentFailures = recentSteps.filter((step) => step.status === "failed").length;

  let health = "healthy";
  if (isStale) {
    health = "stale";
  } else if (["completed", "failed", "blocked", "paused"].includes(task.status)) {
    health = task.status;
  } else if (recentFailures > 0) {
    health = "degraded";
  }

  return {
    health,
    heartbeat_age_seconds: heartbeatAgeSeconds,
    activity_age_seconds: activityAgeSeconds,
    is_stale: isStale,
    recent_fallbacks: recentFallbacks,
    recent_failures: recentFailures,
    step_count: recentSteps.length,
  };
}

function summarizeStepTimeline(steps) {
  const timeline = [];
  for (const step of steps || []) {
    const last = timeline[timeline.length - 1];
    if (last && last.action === step.action) {
      if (step.status === "running" && !last.started_at) {
        last.started_at = step.timestamp;
      }
      if (!last.agent && step.agent) {
        last.agent = step.agent;
      }
      if (!last.resume_hint && step.resume_hint) {
        last.resume_hint = step.resume_hint;
      }
      if (step.status !== "running") {
        last.status = step.status;
        last.completed_at = step.timestamp;
        last.timestamp = step.timestamp;
      }
      const outputs = extractStepOutputs(step);
      if (outputs.length) {
        last.outputs = Array.from(new Set([...(last.outputs || []), ...outputs]));
      }
      if (step.chunk_id && !(last.chunk_ids || []).includes(step.chunk_id)) {
        last.chunk_ids = [...(last.chunk_ids || []), step.chunk_id];
      }
      continue;
    }

    timeline.push({
      ...step,
      started_at: step.status === "running" ? step.timestamp : undefined,
      completed_at: step.status !== "running" ? step.timestamp : undefined,
      outputs: extractStepOutputs(step),
      chunk_ids: step.chunk_id ? [step.chunk_id] : [],
    });
  }
  return timeline;
}

function extractStepOutputs(step) {
  const outputs = [];
  for (const key of ["output", "outputs", "version", "chunks", "claims", "entities", "relations", "events", "conflicts", "placeholders", "count", "sources"]) {
    if (step[key] === undefined || step[key] === null) continue;
    outputs.push(`${key}: ${Array.isArray(step[key]) ? step[key].join(", ") : String(step[key])}`);
  }
  return outputs;
}

function getHeartbeatAgeSeconds(value) {
  if (!value) return null;
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) return null;
  return Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
}

function extractPromptTemplatePath(content) {
  const match = content.match(/^prompt_template:\s*(.+)$/m);
  return match ? match[1].trim() : "";
}

function extractRole(content) {
  const match = content.match(/^role:\s*>([\s\S]*?)(?:\n[a-zA-Z_]+:|\n$)/m);
  if (match) {
    return match[1].replace(/\n/g, " ").trim();
  }
  return "";
}

function toWorkspaceRelative(absolutePath) {
  return path.relative(rootDir, absolutePath) || absolutePath;
}

function sanitizeWorkspaceId(value) {
  return String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .replace(/_+/g, "_");
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf-8") || "{}";
        resolve(JSON.parse(raw));
      } catch (error) {
        reject(error);
      }
    });
    req.on("error", reject);
  });
}

function getActivityAgeSeconds(task, recentSteps) {
  const timestamps = [];
  if (task.last_heartbeat) {
    const parsed = Date.parse(task.last_heartbeat);
    if (!Number.isNaN(parsed)) timestamps.push(parsed);
  }
  for (const step of recentSteps || []) {
    const parsed = Date.parse(step.timestamp || "");
    if (!Number.isNaN(parsed)) timestamps.push(parsed);
  }
  if (!timestamps.length) return null;
  return Math.max(0, Math.floor((Date.now() - Math.max(...timestamps)) / 1000));
}

function summarizeTaskImpact(task, rawSources, multiDb) {
  const sourceIds = collectTaskSourceIds(task, rawSources);
  const sourceIdSet = new Set(sourceIds);
  const databases = (multiDb?.databases || [])
    .map((database) => {
      const touchedRows = (database.rows || []).filter((row) => rowTouchesSources(row, database.name, sourceIdSet));
      if (!touchedRows.length) return null;
      return {
        name: database.name,
        title: database.title || database.name,
        row_count: touchedRows.length,
        sample_rows: touchedRows.slice(0, 3).map((row) => row.name || row.id || "未命名记录"),
      };
    })
    .filter(Boolean);

  return {
    source_count: sourceIds.length,
    source_ids: sourceIds,
    databases,
  };
}

function applyDatabaseVisibility(payload, databasePlan = null) {
  const source = payload && typeof payload === "object" ? payload : {};
  const planMap = new Map(
    (databasePlan?.databases || source.databases || []).map((database) => [database.name, database]),
  );
  const databases = (source.databases || []).map((database) => {
    const planRow = planMap.get(database.name) || {};
    return {
      ...database,
      title: database.title || planRow.title || database.name,
      description: database.description || planRow.description || "",
      visibility: database.visibility || planRow.visibility || inferDatabaseVisibility(database.name),
    };
  });
  return { ...source, databases };
}

function buildSourceSummary(content) {
  const normalized = String(content || "").replace(/\s+/g, " ").trim();
  if (!normalized) return "无内容";
  return normalized.length > 120 ? `${normalized.slice(0, 120)}...` : normalized;
}

function inferDatabaseVisibility(name) {
  return ["claims", "relations", "sources"].includes(name) ? "system" : "business";
}

function collectTaskSourceIds(task, rawSources) {
  if (!Array.isArray(rawSources)) return [];
  const start = Date.parse(task.started_at || "");
  const end = task.ended_at ? Date.parse(task.ended_at) : Date.now();
  return rawSources
    .filter((source) => {
      const timestamp = Date.parse(source.ingested_at || "");
      if (Number.isNaN(timestamp) || Number.isNaN(start)) return false;
      return timestamp >= start && timestamp <= end;
    })
    .map((source) => source.source_id)
    .filter(Boolean);
}

function rowTouchesSources(row, databaseName, sourceIdSet) {
  if (!row || !sourceIdSet.size) return false;
  if (databaseName === "sources" && sourceIdSet.has(row.id)) return true;
  if (sourceIdSet.has(row.source_ref)) return true;
  if (Array.isArray(row.source_refs) && row.source_refs.some((item) => sourceIdSet.has(item))) return true;
  return false;
}

async function handleIngest(req, res, workspaceId) {
  const workspaceRoot = path.join(workspacesDir, workspaceId);
  if (!existsSync(workspaceRoot)) {
    return sendJson(res, { error: "workspace not found" }, 404);
  }

  try {
    const tasks = await readTasks(path.join(workspaceRoot, "tasks"));
    const activeTask = tasks.find((task) => task.status === "running");
    if (activeTask) {
      return sendJson(
        res,
        {
          error: `workspace has an active task: ${activeTask.task_id}`,
          task_id: activeTask.task_id,
          current_step: activeTask.current_step,
        },
        409,
      );
    }

    const { fields, uploads } = await parseIngestRequest(req);
    const sources = buildIngestSources(fields, uploads);
    if (!sources.length) {
      return sendJson(res, { error: "no data provided" }, 400);
    }

    const tempDir = path.join(tmpdir(), "mindvault_ingest_inputs");
    await mkdir(tempDir, { recursive: true });
    const tempPath = path.join(tempDir, `ingest_${workspaceId}_${Date.now()}.json`);
    await writeFile(tempPath, JSON.stringify(sources, null, 2), "utf-8");

    startIngestCommand(workspaceId, tempPath);
    return sendJson(res, {
      success: true,
      accepted: true,
      workspace: workspaceId,
      message: "Ingest started in background. Check Tasks for progress.",
    });
  } catch (error) {
    return sendJson(res, { error: error.message }, 500);
  }
}

function parseIngestRequest(req) {
  return new Promise((resolve, reject) => {
    const busboy = Busboy({ headers: req.headers });
    const fields = {};
    const uploads = [];

    busboy.on("field", (name, value) => {
      fields[name] = value;
    });

    busboy.on("file", (fieldname, file, { filename }) => {
      const buffers = [];
      file.on("data", (chunk) => buffers.push(chunk));
      file.on("end", () => {
        uploads.push({
          fieldname,
          filename,
          content: Buffer.concat(buffers).toString("utf-8"),
        });
      });
    });

    busboy.on("error", reject);
    busboy.on("finish", () => resolve({ fields, uploads }));
    req.pipe(busboy);
  });
}

function buildIngestSources(fields, uploads) {
  const sources = [];
  const contextHints = {};
  if (fields.target_db) contextHints.target_db = fields.target_db;
  if (fields.new_db_name) contextHints.new_db_name = fields.new_db_name;
  if (fields.note) contextHints.note = fields.note;

  if (fields.text_input) {
    sources.push({
      source_id: `text_${Date.now()}`,
      source_type: "doc",
      content: fields.text_input,
      context_hints: contextHints,
    });
  }

  if (fields.json_input) {
    try {
      const parsed = JSON.parse(fields.json_input);
      if (Array.isArray(parsed)) {
        parsed.forEach((item, index) => {
          sources.push({
            ...item,
            source_id: item.source_id || `json_array_${index}_${Date.now()}`,
            context_hints: {
              ...(item.context_hints || {}),
              ...contextHints,
            },
          });
        });
      } else if (typeof parsed === "object" && parsed !== null) {
        sources.push({
          ...parsed,
          source_id: parsed.source_id || `json_${Date.now()}`,
          context_hints: {
            ...(parsed.context_hints || {}),
            ...contextHints,
          },
        });
      }
    } catch (err) {
      sources.push({
        source_id: `json_invalid_${Date.now()}`,
        source_type: "doc",
        content: fields.json_input,
        context_hints: { ...contextHints, parse_error: err.message },
      });
    }
  }

  uploads.forEach((upload, index) => {
    sources.push({
      source_id: `upload_${index}_${Date.now()}`,
      source_type: "doc",
      content: upload.content,
      metadata: { filename: upload.filename },
      context_hints: contextHints,
    });
  });

  return sources;
}

function startIngestCommand(workspaceId, inputPath) {
  const python = spawn("python3", ["-m", "mindvault.runtime.app", "-w", workspaceId, "-i", inputPath], {
    cwd: rootDir,
    detached: true,
    stdio: "ignore",
  });
  python.unref();
}
