# Self-Growing Knowledge Base (MindVault)

这是一个**完整、可运行、可持续增长**的知识库系统，采用**多 Agent 任务网格（Task Mesh）**，并支持**Workspace（工作空间）隔离**。

## 你关心的两件事（已实现）

1. **可视化可直接看数据库结果**
   - 每次运行都会生成：
     - `graph_data.json`（关系图节点/边）
     - `dashboard.html`（可直接打开查看实体、事件、关系、KPI）
2. **Agent 能互相传递任务，方便二次编辑**
   - 使用 `workflow/default_workflow.json` 定义任务类型到 Agent 的路由。
   - 你只要修改这个 JSON，就能调整编排顺序或替换 Agent。

## 架构设计

### 1) Task Mesh 多 Agent 编排

核心文件：
- `agent_mesh.py`：任务队列、路由、执行追踪
- `agent_runtime.py`：注册各 Agent handler，并通过任务流串起来
- `workflow/default_workflow.json`：可编辑工作流

任务流默认如下：

`ingest.start -> parse.request -> dedup.request -> relation.request -> placeholder.request -> kb.merge.request -> insight.request -> version.request -> report.request -> visualize.request`

### 2) Workspace 隔离

通过 `WorkspaceManager` 实现：

- `output/workspaces/<workspace_id>/knowledge_base.json`
- `output/workspaces/<workspace_id>/snapshots/kb_snapshot_v*.json`
- `output/workspaces/<workspace_id>/report.md`
- `output/workspaces/<workspace_id>/visuals/dashboard.html`
- `output/workspaces/<workspace_id>/visuals/graph_data.json`
- `output/workspaces/<workspace_id>/agent_trace.json`

## 项目结构

```text
project/
├── agent_mesh.py
├── agent_runtime.py
├── workflow/
│   └── default_workflow.json
├── workspace_manager.py
├── ingestor.py
├── parser.py
├── deduplicator.py
├── relation_builder.py
├── knowledge_base.py
├── placeholder_manager.py
├── version_manager.py
├── insight_generator.py
├── visualizer.py
├── main.py
├── sample_data/
│   └── raw_inputs.json
├── output/
├── requirements.txt
└── README.md
```

## 运行

```bash
python3 main.py --workspace demo --input sample_data/raw_inputs.json
```

## 二次编辑建议

- **改编排顺序**：编辑 `workflow/default_workflow.json`。
- **替换单个 Agent**：在 `agent_runtime.py` 的 `_register_handlers()` 中替换对应 handler。
- **扩展新任务**：新增 `task_type` 与 handler，并在 workflow 里加路由。
