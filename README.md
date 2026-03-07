# MindVault v0.2

MindVault 已从“可运行流水线”升级为“可治理的自增长知识系统”。

## 核心升级

- Claim 中间层：parser 先产出 claims + candidates，再进入 merge。
- 统一置信度：claims/entities/events/relations 都带 `confidence/source_refs/status/updated_at`。
- 冲突审计：自动生成 `governance/conflicts.json`。
- Schema 演化：新字段先进入候选池，再按阈值晋升。
- Placeholder 生命周期：结构化对象与状态流转。
- 版本管理升级：每次运行同时生成 snapshot + changelog。
- 分层目录：`raw/extracted/canonical/snapshots/reports/visuals/governance/config`。
- LLM 抽象层：新增 OpenAI-compatible client/router 与模型路由配置。
- Dashboard 治理化：新增 KPI、冲突、placeholder、schema candidates、version diff、agent trace 面板。
- 最小 benchmark + regression tests。

## 目录结构

```text
output/workspaces/<workspace_id>/
├── raw/
├── extracted/
├── canonical/
├── snapshots/
├── reports/
├── visuals/
├── governance/
└── config/
```

## 运行

```bash
python3 main.py --workspace demo --input sample_data/raw_inputs.json
```

## 关键产物

- `extracted/claims_v*.json`
- `canonical/knowledge_base.json`
- `canonical/schema.json`
- `canonical/taxonomy.json`
- `governance/conflicts.json`
- `governance/placeholders.json`
- `governance/schema_candidates.json`
- `snapshots/kb_snapshot_v*.json`
- `snapshots/changelog_v*.json`
- `visuals/dashboard.html`
- `visuals/graph_data.json`

## 模型配置（OpenAI-compatible）

使用 `config/model_config.json`：

- `base_url`
- `api_key_env`
- `model`
- `routing`（parse/insight/report）

环境变量示例：

```bash
export OPENAI_API_KEY=your_key
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 示例数据集

- `sample_data/benchmarks/semi_structured.json`
- `sample_data/benchmarks/noisy_chat.json`
- `sample_data/benchmarks/conflicting_multi_source.json`
