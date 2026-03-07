# Self-Growing Knowledge Base (MindVault)

一个完整、可运行的自增长知识库项目，覆盖从数据投喂到可视化输出的全流程：

1. Ingestor Agent（输入归一化）
2. Parser Agent（实体/事件/关系抽取）
3. Schema Designer Agent（动态 schema 设计）
4. Deduplicator Agent（去重合并）
5. Relation Builder Agent（关系补全）
6. Placeholder Manager Agent（占位字段管理）
7. Version Manager Agent（版本快照与 diff）
8. Insight Generator Agent（洞察与报告）
9. Visualizer Agent（图谱与统计图）

## 项目结构

```text
project/
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
├── output/                 # 运行后自动生成
├── requirements.txt
└── README.md
```

## 运行方式

```bash
python3 -m pip install -r requirements.txt
python3 main.py
```

运行后会生成：
- `output/knowledge_base.json`
- `output/kb_snapshot_v*.json`
- `output/report.md`
- `output/knowledge_graph.png`
- `output/entity_distribution.png`

## 示例输出要求覆盖

初始样例可保证至少包含：
- 2+ entities
- 1+ event
- 1+ relation
- 1+ insight
- placeholders 字段

并自动给出报告文本和可视化图像。
