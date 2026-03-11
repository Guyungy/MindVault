# MindVault Agent Architecture

## Goal

MindVault now uses a simplified four-agent architecture for user-facing control.
The system keeps low-level implementation details internal, but the visible
mental model is:

1. `解析智能体`
2. `治理智能体`
3. `建库智能体`
4. `输出智能体`

This mirrors the OpenClaw-style principle of keeping coordination simple and
keeping user-facing agent roles stable.

## Top-level agents

### 解析智能体

- Purpose: turn raw input into structured candidate knowledge
- Owns: extraction, relation recognition, fallback parsing
- Agent dir: `mindvault/agents/parsing/`

### 治理智能体

- Purpose: clean, reconcile, score, and stabilize knowledge
- Owns: confidence scoring, schema stabilization, merge, conflict governance
- Agent dir: `mindvault/agents/governance/`

### 建库智能体

- Purpose: design useful tables and build the final multi-table result
- Owns: ontology planning, multi-table projection
- Agent dir: `mindvault/agents/modeling/`

### 输出智能体

- Purpose: generate optional summaries and reports without blocking core tables
- Owns: insight/report output
- Agent dir: `mindvault/agents/publishing/`

## Runtime split

MindVault is intentionally split by responsibility:

- Node.js:
  - Web UI
  - workspace CRUD
  - runtime settings
  - skill management
  - task presentation
  - product-facing orchestration controls

- Python:
  - parsing
  - governance
  - LLM-based table building
  - canonical knowledge state
  - JSON artifacts

Python should behave like a knowledge engine, not a UI renderer.

## Why four visible agents

The system still has internal implementation steps, but they are not first-class
user objects anymore. Users should not need to understand a chain of tiny
sub-agents to operate the product.

The UI therefore exposes only the four top-level agents, each with one
OpenClaw-style document bundle:

- `SOUL.md`
- `AGENTS.md`
- `TOOLS.md`
- `HEARTBEAT.md`
- `MEMORY.md`

and shared skills plus one stable role in the system.

## Soul-driven behavior

Each top-level agent now has an agent workspace directory under `mindvault/agents/`.

At runtime, internal implementation calls inherit the full guide bundle
(`SOUL.md`, `AGENTS.md`, `TOOLS.md`, `HEARTBEAT.md`, `MEMORY.md`) of their
top-level agent group through `AgentExecutor`.

This keeps editing simple while preserving implementation flexibility.

## Settings ownership

System-level controls are owned by Node settings:

- execution profile (`fast` / `full`)
- optional artifacts (`report`)
- model routing
- skills

Python reads these settings and executes accordingly, but does not own the UI
control surface.
