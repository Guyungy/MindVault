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

## 运行指南

### 1. 环境准备

项目代码使用 Python 编写，在运行前需要安装相应的依赖。

```bash
pip install -r requirements.txt
```

依赖项包括：
- `pandas>=2.0.0`
- `networkx>=3.0`
- `matplotlib>=3.7.0`

### 2. 模型配置（OpenAI-compatible）

系统依赖 LLM（大语言模型）来提取和处理知识。您需要配置 OpenAI（或兼容的 API）的环境变量。

```bash
export OPENAI_API_KEY=your_key
```

进阶模型配置可在 `config/model_config.json` 中修改：
- `base_url`
- `api_key_env`
- `model`
- `routing`（parse/insight/report 等路由策略）

### 3. 运行主干流水线

完成配置后，可以直接利用内置的样本数据集运行项目：

```bash
python3 main.py --workspace demo --input sample_data/raw_inputs.json
```

**参数说明：**
- `--workspace`：指定工作区名称（比如 `demo`），用于隔离不同的知识库、版本及报表状态。
- `--input`：输入源文件路径。
- `--workflow`：默认为 `workflow/default_workflow.json`（可定制化的路由流水线配置文件）。

## 核心产物

运行成功后，工作区将生成以下分层治理数据：

- **原始与提取层**
  - `extracted/claims_v*.json`
- **规范标准层**
  - `canonical/knowledge_base.json`
  - `canonical/schema.json`
  - `canonical/taxonomy.json`
- **治理与冲突**
  - `governance/conflicts.json`
  - `governance/placeholders.json`
  - `governance/schema_candidates.json`
- **版本抓拍层**
  - `snapshots/kb_snapshot_v*.json`
  - `snapshots/changelog_v*.json`
- **可视化结果**
  - `visuals/dashboard.html`
  - `visuals/graph_data.json`

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 示例数据集

- `sample_data/benchmarks/semi_structured.json`
- `sample_data/benchmarks/noisy_chat.json`
- `sample_data/benchmarks/conflicting_multi_source.json`
