<<<<<<< HEAD
# Evidence-Based Triage Backend — one-file package

One self-contained file — `triage_backend_ONE_FILE.py` — that is the entire
backend: 9 interconnected AI agents, an 11-phase workflow, Bend aggregation
(real `.bend` kernels with a deterministic Python fallback), a Fetch.ai-style
assistant (plus optional `uagents` SDK bridge), env-only secrets, and a
frontend-ready HTTP API.

## 1. Quickstart

```bash
pip install fastapi "uvicorn[standard]" httpx pydantic "sqlalchemy>=2" aiosqlite
python triage_backend_ONE_FILE.py            # API on http://127.0.0.1:8080, docs at /docs
# or: uvicorn triage_backend_ONE_FILE:app --reload
```

No configuration required for the first run: without any API keys the backend
is fully offline and deterministic (heuristic agents, Python Bend fallback,
SQLite). Add keys only when you want real providers.

## 2. What a frontend gets

| Need | Endpoint |
| --- | --- |
| Start a triage | `POST /api/v1/analyses` → `202 {"analysis_id", "accepted"}` |
| Live progress | `GET /api/v1/analyses/{id}/events` — SSE (`analysis.status`, `agent.run`, `workflow.done`) |
| Claims + confidence | `GET /api/v1/analyses/{id}/claims` |
| Evidence per claim | `GET /api/v1/claims/{claim_id}/evidence` |
| Final report | `GET /api/v1/analyses/{id}/report?format=md|json` |
| Per-agent trace | `GET /api/v1/analyses/{id}/trace` |
| Human feedback | `POST /api/v1/analyses/{id}/feedback` (recorded, never auto-applied) |
| Chat assistant | `POST /api/v1/assistant/chat` `{analysis_id, question}` |
| Capabilities | `GET /api/v1/meta` (agents, phases, endpoints, auth posture) |
| Demo payload | `GET /api/v1/sample/issue` |
| GitHub ingestion | `POST /webhooks/github` (HMAC-verified, idempotent) |
| Health / metrics | `GET /health`, `GET /ready`, `GET /metrics` |

CORS is on by default (`CORS_ORIGINS=*`). For production set
`CORS_ORIGINS=https://your-frontend.example` in `.env`.

## 3. Copy-paste frontend snippets

```js
// 1. start an analysis
const { analysis_id } = await fetch("http://localhost:8080/api/v1/analyses", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({
    repository: "octocat/hello-world",
    title: "Crash on empty payload (v2.4.1)",
    body: "POST /api/webhooks with {} returns 500. Regression from v2.3.9. Docs say 422.",
    subject_type: "issue",
    reported_version: "2.4.1",
  }),
}).then(r => r.json());

// 2. live pipeline progress (9 agent events + status changes)
const es = new EventSource(`http://localhost:8080/api/v1/analyses/${analysis_id}/events`);
es.addEventListener("agent.run", e => renderAgent(JSON.parse(e.data)));
es.addEventListener("workflow.done", e => { es.close(); loadReport(analysis_id); });

// 3. fetch results when done
const claims = await fetch(`http://localhost:8080/api/v1/analyses/${analysis_id}/claims`).then(r => r.json());
const report = await fetch(`http://localhost:8080/api/v1/analyses/${analysis_id}/report?format=md`).then(r => r.json());

// 4. chat with the grounded assistant
const answer = await fetch("http://localhost:8080/api/v1/assistant/chat", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ analysis_id, question: "Which claims are confirmed and why?" }),
}).then(r => r.json());   // { answer, citations, grounded }
```

## 4. Secrets — hidden by design

Every key is read from the environment (`.env`), never hardcoded, scrubbed from
logs, and never injected into prompts. On first run `.env.example` is written
next to the backend file — copy it to `.env` and fill in only what you use:
`DATABASE_URL`, `ADMIN_API_TOKEN`, `REQUIRE_ADMIN_TOKEN`, `CORS_ORIGINS`,
`GITHUB_WEBHOOK_SECRET`, `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY_PEM`,
`LLM_API_KEY`/`LLM_BASE_URL`/`LLM_MODEL`, `BEND_URL`, `COMMENT_MODE`.

## 5. Opening in Google Antigravity (step by step)

1. Install Antigravity from `antigravity.google` (free, VS Code fork), sign in
   with a Google account.
2. Put `triage_backend_ONE_FILE.py` in an empty folder.
3. In Antigravity: **File → Open Folder…** → select the folder → **Trust**.
4. Terminal (Ctrl+`): install deps, then `python triage_backend_ONE_FILE.py`.
5. Verify `http://127.0.0.1:8080/docs` — Swagger UI; `GET /health` → ok.
6. Secrets: copy `.env.example` → `.env`; offline mode needs nothing.
7. Frontend: switch to **Agent Manager** and prompt: *"Read AGENTS.md and
   ANTIGRAVITY_README.md, then build a React dashboard in `frontend/` that
   starts analyses via POST /api/v1/analyses, streams progress from the SSE
   events endpoint, and renders the report."* The agent can read
   `http://127.0.0.1:8080/openapi.json` for the exact contract.

## 6. Safety invariants

- Evidence is data, never instructions — all untrusted text passes an
  injection scanner before agents see it.
- The tool surface is read-only by construction: no issue closing, no pushes,
  no label writes.
- The system never judges whether an issue author is human or AI.
- Human feedback is recorded, never silently applied.
=======
# Commit-Criminals
This is an AI-powered tool that verifies GitHub issues and pull requests against actual source code and documentation, highlighting evidence, inconsistencies, and missing information to help open-source maintainers make faster, more informed decisions.
>>>>>>> 779c8931701bb123d0369ade6775a399f7d89cd7
