import { createServer } from "node:http";
import { readFile, readdir, stat } from "node:fs/promises";
import { createReadStream, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(__dirname, "dist");
const publicDir = existsSync(distDir) ? distDir : path.join(__dirname, "public");
const workspacesDir = path.join(rootDir, "output", "workspaces");
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
    return sendJson(res, await listWorkspaces());
  }

  if (url.pathname.startsWith("/api/workspaces/")) {
    const workspaceId = decodeURIComponent(url.pathname.replace("/api/workspaces/", "").replace(/\/$/, ""));
    return sendJson(res, await readWorkspacePayload(workspaceId), 200);
  }

  return serveStatic(url.pathname, res);
});

server.listen(port, host, () => {
  console.log(`MindVault frontend: http://${host}:${port}`);
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

async function readWorkspacePayload(workspaceId) {
  const workspaceRoot = path.join(workspacesDir, workspaceId);
  const multiDbPath = path.join(workspaceRoot, "multi_db", "multi_db.json");
  const planPath = path.join(workspaceRoot, "multi_db", "database_plan.json");
  const tracePath = path.join(workspaceRoot, "agent_trace.json");
  const tasks = await readTasks(path.join(workspaceRoot, "tasks"));
  const latestTask = tasks[0] || null;

  if (!existsSync(workspaceRoot)) {
    return { error: `Workspace not found: ${workspaceId}` };
  }

  const [multiDb, plan, trace] = await Promise.all([
    readJsonSafe(multiDbPath, { databases: [], relations: [] }),
    readJsonSafe(planPath, { databases: [], relations: [] }),
    readJsonSafe(tracePath, []),
  ]);

  return {
    workspace: workspaceId,
    multiDb,
    databasePlan: plan,
    trace,
    tasks,
    latestTask,
  };
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
    const recentSteps = await readStepLogTail(stepLogPath, 6);
    if (task) tasks.push({ ...task, recentSteps, monitor: summarizeTask(task, recentSteps) });
  }
  return tasks;
}

async function readStepLogTail(stepLogPath, maxItems) {
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
  const isStale = task.status === "running" && heartbeatAgeSeconds !== null && heartbeatAgeSeconds > 300;
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
    is_stale: isStale,
    recent_fallbacks: recentFallbacks,
    recent_failures: recentFailures,
    step_count: recentSteps.length,
  };
}

function getHeartbeatAgeSeconds(value) {
  if (!value) return null;
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) return null;
  return Math.max(0, Math.floor((Date.now() - timestamp) / 1000));
}
