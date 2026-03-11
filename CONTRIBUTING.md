# Contributing to MindVault

Thanks for your interest in contributing!

## Quick start (dev)

### 1) Backend (Python)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

### 2) Frontend (Node)

```bash
cd frontend
npm install
npm run dev
```

Default URL:

- http://localhost:4310

## How to contribute

- **Bug reports**: please open an issue with steps to reproduce + logs.
- **Feature requests**: describe the use case, expected behavior, and sample data.
- **PRs**: keep them small and focused. Include tests when practical.

## PR checklist

- [ ] Clear description of the change and why it matters
- [ ] Tests added/updated (or explain why not)
- [ ] Documentation updated (README/docs)
- [ ] `pytest -q` passes

## Coding style

- Prefer readable, explicit code.
- Keep business logic in `mindvault/`.
- Avoid breaking public CLI flags without a deprecation note.
