# MindVault v2 重构 · 第二阶段验证复核（仓库现状）

## 结论

基于当前仓库内可见配置、代码与运行日志，原“第二阶段验证报告”**部分准确、部分无法证实，且核心压力测试结论与仓库日志不一致**。

- ✅ **准确项**：`parse` 路由确实指向 `gpt-5.2` 的 `gpt52_model`，基础地址为 `http://34.124.175.101:8371/v1`。
- ✅ **准确项**：记忆治理层（Memory Curator）已采用 `confidence` 缺省值 `0.6`，且 `min_sources=0`，符合“放宽拦截条件”的描述。
- ⚠️ **部分准确**：LLM Client 有超时 `120s` 和 fallback 机制，但默认未启用重试（`max_retries=0`），与“重试优化”描述不完全一致。
- ⚠️ **部分准确**：`AgentExecutor` 对 ```json 与普通 ``` 代码块均有 JSON 提取逻辑，但对“非标准包裹字符串”的鲁棒性有限（如多个代码块、闭合异常等仍可能失败）。
- ❌ **不准确/无法证实项**：仓库内压力测试日志 `test_run.log` 显示 `test.md` 运行时连续 502 fallback，最终 `claims/entities/relations/events` 全为 `0`，与“31/23/22/3”统计明显冲突。
- ❌ **无法证实项**：仓库中未找到报告提到的 `v2_live` 工作区产物文件，无法对对应统计做复算。

## 证据索引（快速）

- 模型接入与路由：`config/model_config.json`
- parse 代理路由：`mindvault/agents/parse_agent.yaml`
- 记忆治理阈值逻辑：`mindvault/governance/memory_curator.py`
- JSON 解析容错：`mindvault/runtime/agent_executor.py`
- LLM 超时/fallback：`mindvault/runtime/llm_client.py`
- 现有压力测试日志（502 + 全 0）：`test_run.log`

## 建议

1. 若要让“31/23/22/3”结论成立，请补充当次运行的可复现工件（`output/workspaces/v2_live/...` 下 extracted/canonical/trace/snapshot）。
2. 将 `PyYAML` 增加到依赖清单，避免 v2 CLI 在新环境中因缺依赖无法运行。
3. 若要声明“已重试优化”，建议把 `max_retries` 暴露到配置并在 parse 路由中显式设置（当前默认为 0）。
