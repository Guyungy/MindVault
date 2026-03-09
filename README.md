# MindVault v0.2

MindVault 是一个**可治理（governable）**、**可追溯（traceable）**、**可持续演化（self-growing）**的知识系统。
它把多来源原始文本（聊天、表格、文档）转为结构化知识，并在每次运行后输出：
- 规范化知识库（Canonical KB）
- 冲突与占位符治理结果（Governance）
- 版本快照与变更记录（Snapshot + Changelog）
- 报告与可视化面板（Report + Dashboard）

---

## 1. 项目目标

传统“抽取即结束”的流水线通常存在以下问题：
1. 缺少中间层，错误难以定位。
2. 冲突值无法治理（例如价格多版本来源冲突）。
3. 无版本追踪，无法解释“知识如何演化”。
4. 输出可读性差，业务与技术角色难协作。

MindVault v0.2 的目标是：
- 让知识抽取结果**可审计**（来源、置信度、状态都可追踪）。
- 让冲突与缺失信息**可治理**（冲突文件 + placeholder 生命周期）。
- 让知识状态**可回放**（每次运行生成快照与变更）。
- 让结果**人机双可读**（Markdown 报告 + dashboard）。

---

## 2. 核心能力一览

- **Claim 中间层**：先形成 claims/candidates，再合并到 canonical。
- **统一置信度模型**：实体/事件/关系/claim 统一支持 `confidence/source_refs/status/updated_at`。
- **冲突审计**：输出 `governance/conflicts.json`。
- **Schema 演化**：新字段先进入候选池，再按阈值晋升。
- **Placeholder 生命周期**：缺失字段可持续跟踪。
- **版本管理**：每次运行生成 snapshot + changelog。
- **工作区隔离**：不同 workspace 的知识状态互不污染。
- **LLM 抽象层**：支持 OpenAI-compatible client/router。
- **治理可视化**：dashboard 展示 KPI、冲突、placeholder、schema candidates、version diff、agent trace。

---

## 3. 目录结构

### 3.1 源码目录（简化）

```text
.
├── main.py                          # 入口：运行 pipeline
├── agent_runtime.py                 # 多 agent 编排
├── parser.py                        # 抽取逻辑（claims/candidates）
├── insight_generator.py             # 报告洞察生成
├── workspace_manager.py             # 工作区路径管理
├── version_manager.py               # 快照与变更记录
├── mindvault/
│   ├── runtime/                     # runtime 组件（store/renderers/router 等）
│   ├── governance/                  # 冲突/placeholder/schema 演化治理
│   ├── adapters/                    # 多源输入适配
│   └── agents/                      # agent 配置
├── tests/                           # 单元测试 / 回归测试
└── output/                          # 运行产物
```

### 3.2 工作区产物目录

```text
output/workspaces/<workspace_id>/
├── raw/                             # 原始输入快照
├── extracted/                       # claims / candidates 等中间抽取结果
├── canonical/                       # 规范化知识库（最终态）
├── snapshots/                       # 版本快照与 changelog
├── reports/                         # Markdown 报告
├── visuals/                         # dashboard/graph 数据
├── governance/                      # 冲突、placeholder、schema 候选
└── config/                          # 运行相关配置镜像
```

---

## 4. 快速开始（5 分钟）

### 4.1 环境要求

- Python 3.10+（建议）
- pip

### 4.2 安装依赖

```bash
pip install -r requirements.txt
```

### 4.3 配置 LLM（OpenAI-compatible）

```bash
export OPENAI_API_KEY=your_key
```

可选：在 `config/model_config.json` 中自定义
- `base_url`
- `api_key_env`
- `model`
- `routing`（不同 agent/阶段的模型路由）

> 如果只想先跑通基本流程，可先使用项目已有样例输入做本地验证。

### 4.4 运行主流水线

```bash
python3 -m mindvault.runtime.app -w demo -i sample_data/benchmarks/semi_structured.json
```

参数说明：
- `--workspace`：工作区 ID（隔离多次实验与状态）。
- `--input`：输入文件或目录路径，支持 `.md`、`.txt`、`.json`。
- `--config`：配置目录路径，默认 `mindvault/config`。

运行成功后，终端会打印各产物路径（knowledge_base、report、dashboard、wiki 等）。

### 4.5 批量处理一个文件夹

系统现在支持直接输入一个目录，自动递归处理其中所有 `.md`、`.txt`、`.json` 文件。

```bash
python3 -m mindvault.runtime.app -w demo -i /Users/a1/Documents/GitHub/MindVault/data
```

例如处理当前项目内的某个资料目录：

```bash
python3 -m mindvault.runtime.app -w demo -i /Users/a1/Documents/GitHub/MindVault/sample_data
```

目录模式下的行为：
- 递归扫描子目录。
- 只读取 `.md`、`.txt`、`.json` 文件。
- `.json` 支持单个对象或对象数组。
- 其他文件类型会被自动忽略。

---

## 5. 输入输出说明

### 5.1 输入

典型输入位于 `sample_data/`，可包含：
- 半结构化文本
- 噪声聊天记录
- 多来源冲突数据
- 目录形式的批量资料集

推荐先用 benchmark 文件验证：
- `sample_data/benchmarks/semi_structured.json`
- `sample_data/benchmarks/noisy_chat.json`
- `sample_data/benchmarks/conflicting_multi_source.json`

也可以直接传入目录：

```bash
python3 -m mindvault.runtime.app -w demo -i sample_data/benchmarks
```

### 5.2 输出（重点文件）

- **抽取层**
  - `extracted/claims_v*.json`
- **规范层**
  - `canonical/knowledge_base.json`
  - `canonical/schema.json`
  - `canonical/taxonomy.json`
- **治理层**
  - `governance/conflicts.json`
  - `governance/placeholders.json`
  - `governance/schema_candidates.json`
- **版本层**
  - `snapshots/kb_snapshot_v*.json`
  - `snapshots/changelog_v*.json`
- **展示层**
  - `reports/report.md`
  - `visuals/dashboard.html`
  - `visuals/graph_data.json`
  - `wiki/index.md`
  - `wiki/governance.md`
  - `wiki/tables.json`

---

## 6. Pipeline 数据流（概念）

1. **Ingest**：读入原始输入，沉淀到 `raw/`。
2. **Parse/Extract**：生成 claims + entity/event/relation candidates。
3. **Merge**：合并到 canonical KB，去重并更新时间。
4. **Governance**：执行冲突检测、placeholder 追踪、schema 候选管理。
5. **Insight/Report**：生成洞察、Markdown 报告。
6. **Versioning**：写入 snapshot 与 changelog。
7. **Visualization**：生成 dashboard 与图谱数据。

---

## 7. 测试与质量验证

### 7.1 运行全部测试

```bash
python3 -m unittest discover -s tests -v
```

### 7.2 关键回归点

当前测试重点覆盖：
- claim 抽取与分层产物是否生成。
- 噪声数据中的低置信度 claim 是否被保留。
- 多源冲突场景中是否识别到关键字段冲突（如 `price`）。

---

## 8. 常见问题（FAQ）

### Q1：为什么没有生成报告？
- 先检查 `python3 -m mindvault.runtime.app ...` 的终端输出路径。
- 确认 `output/workspaces/<workspace>/reports/report.md` 是否存在。
- 若 pipeline 中断，优先查看 `agent_trace.json` 与测试输出。

### Q6：如何一次处理整个文件夹？
- 直接把目录传给 `-i`。
- 例如：`python3 -m mindvault.runtime.app -w demo -i sample_data/benchmarks`
- 系统会递归读取其中的 `.md`、`.txt`、`.json` 文件。

### Q2：如何避免不同实验相互污染？
- 始终使用不同 `--workspace`（例如 `demo_v1`, `demo_v2`）。

### Q3：冲突值在哪里看？
- `governance/conflicts.json`。
- dashboard 中通常也有冲突面板。

### Q4：字段缺失（placeholder）在哪里看？
- `governance/placeholders.json`。
- `reports/report.md` 中的 Placeholder Focus 区域。

### Q5：如何接入 OpenAI 兼容服务（非官方域名）？
- 在 `config/model_config.json` 调整 `base_url` 与模型路由。
- 确保 `api_key_env` 对应环境变量已设置。

---

## 9. 开发建议

- 新增能力优先通过 `mindvault/runtime` 与 `mindvault/governance` 扩展，避免把复杂逻辑堆在入口层。
- 任何会改变 canonical 结果的改动，建议同步补充 tests 回归场景。
- 提交前建议至少执行一次：
  1) benchmark 测试
  2) smoke run
  3) 报告与治理文件人工抽查

---

## 10. 路线图（建议）

- 更细粒度的 claim provenance（字段级证据链）。
- schema 演化审批工作流（人工审核 + 自动阈值）。
- dashboard 交互增强（冲突 drill-down、变更时间轴）。
- 多租户/多项目规模化部署支持。

---

## 11. License / 备注

如需在生产环境部署，请补充：
- 配置管理与密钥治理（Secrets）
- 观测系统（日志、指标、告警）
- 数据合规与访问控制策略
