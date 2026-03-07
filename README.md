# Self-Growing Knowledge Base (MindVault)

这是一个**完整、可运行、可持续增长**的知识库系统，采用**多 Agent 驱动架构**，并支持**Workspace（工作空间）隔离**。

## 架构（先思考后的设计）

### 1) 多 Agent 驱动层
由 `MultiAgentRuntime` 统一编排，按固定链路执行：

1. Ingestor Agent：原始输入归一化
2. Parser Agent：抽取实体/事件/关系
3. Schema Designer Agent：动态 schema 输出
4. Deduplicator Agent：去重合并
5. Relation Builder Agent：关系补全
6. Placeholder Manager Agent：占位符追踪
7. Knowledge Base Merge：状态合并
8. Insight Generator Agent：洞察生成与报告
9. Version Manager Agent：版本快照与 diff
10. Visualizer Agent：可视化

### 2) Workspace 层
通过 `WorkspaceManager` 实现同一系统下多工作空间数据隔离：

- `output/workspaces/<workspace_id>/knowledge_base.json`
- `output/workspaces/<workspace_id>/snapshots/kb_snapshot_v*.json`
- `output/workspaces/<workspace_id>/report.md`
- `output/workspaces/<workspace_id>/visuals/*`
- `output/workspaces/<workspace_id>/agent_trace.json`

这让不同团队、项目、业务线可以并行运行，不会相互污染数据。

## 项目结构

```text
project/
├── agent_runtime.py
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
├── output/                         # 运行后自动生成
├── requirements.txt
└── README.md
```

## 运行

```bash
python3 main.py --workspace demo
```

可选参数：

```bash
python3 main.py --workspace team_a --input sample_data/raw_inputs.json
```

## 输出

执行后会在对应 workspace 下生成：

- `knowledge_base.json`
- `snapshots/kb_snapshot_v*.json`
- `report.md`
- `visuals/knowledge_graph.png`（依赖可用时）
- `visuals/entity_distribution.png`（依赖可用时）
- 或 `visuals/visualization_fallback.json`（依赖缺失时）
- `agent_trace.json`（记录每个 Agent 的输入输出摘要）
