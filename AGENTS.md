# AGENTS.md — guidance for AI agents working in this repo (Antigravity / Gemini, Cursor, etc.)

## What this project is
A self-contained evidence-based triage backend for GitHub issues/PRs in ONE file:
`triage_backend_ONE_FILE.py` (FastAPI). 9 interconnected agents, 11-phase workflow,
Bend aggregation with deterministic Python fallback, Fetch.ai-style assistant,
frontend-ready REST + SSE API. **Do not split or restructure the single file.**

## Run (always offline-first)
```bash
python triage_backend_ONE_FILE.py    # http://127.0.0.1:8080  (docs at /docs)
```
No API keys are needed — without keys everything runs deterministic/offline
(heuristic agents, Python Bend fallback, SQLite).

## API contract (frontend integration)
- Machine-readable OpenAPI: `http://localhost:8080/openapi.json` — generate typed
  clients from this, do not invent endpoints.
- Key endpoints:
  - `POST /api/v1/analyses` -> 202 `{"analysis_id"}` (start triage)
  - `GET /api/v1/analyses/{id}` · `/claims` · `/report` · `/trace`
  - `GET /api/v1/analyses/{id}/events` — SSE progress (`agent.run`, `workflow.done`)
  - `POST /api/v1/assistant/chat` `{analysis_id, question}`
  - `GET /health`, `GET /ready`, `GET /api/v1/meta`

## Hard rules
1. **Secrets only from env / `.env`** (see `.env.example`). Never hardcode keys,
   tokens or passwords in source, tests or examples. Never commit `.env`.
2. **Never auto-close issues / never post judgmental comments about authors.**
   `COMMENT_MODE=disabled|report_only` only.
3. CORS defaults to `*` for development; for a deployed frontend set
   `CORS_ORIGINS=https://your-frontend` in `.env`.
4. When asked to build a frontend, consume the endpoints above; poll
   `GET /api/v1/analyses/{id}` or subscribe to the SSE stream; render the report
   from `GET .../report?format=json` (fields are stable).

## Verify your changes
```bash
python -c "import triage_backend_ONE_FILE as m; m.create_app(); print('ok')"
# then hit /health and POST /api/v1/analyses with the sample from GET /api/v1/sample/issue
```
