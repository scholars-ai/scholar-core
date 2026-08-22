# Workflow Visualization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a real workflow run/event model, manual cascade trigger, event query/SSE APIs, and a visual homepage that renders the live content pipeline.

**Architecture:** A workflow run uses one UUID as its correlation ID across source fetch, Topic, evaluation, and article jobs. PostgreSQL stores immutable ordered events and artifact references; the API exposes historical JSON plus an SSE stream that polls by sequence for reconnect-safe updates. The Next.js homepage uses `@xyflow/react` with a fixed DAG and an inspector driven by the same event reducer used for live updates.

**Tech Stack:** Go, PostgreSQL/goose/sqlc, OpenAPI/oapi-codegen, Python worker, Next.js 15, React 19, TypeScript, `@xyflow/react`, SSE.

---

### Task 1: Add workflow persistence and API contracts

**Files:** `internal/db/migrations/00010_workflow_visualization.sql`, `internal/db/queries/workflow.sql`, `openapi/core.yaml`, generated Go/TypeScript/Python contract files.

- [ ] Add `workflow_runs`, `workflow_events`, and `workflow_artifacts` with UUID correlation, monotonically increasing event sequence, JSON payloads, and indexes for run lookup.
- [ ] Add request/response schemas for creating runs, listing runs/events/artifacts, and SSE event payloads.
- [ ] Run `make generate` and verify generated files contain the new operations.

### Task 2: Implement manual cascade and workflow APIs

**Files:** `internal/api/workflow_handlers.go`, `internal/api/handler.go`, generated API router, `internal/harvester/harvester.go`, `internal/db/queries/workflow.sql`.

- [ ] Create a run with `source_fetch` as the default start node, attach the run UUID as `correlationId`, and enqueue selected or all enabled sources atomically.
- [ ] Record queued/started/completed/failed node events and artifact references at API and harvester boundaries.
- [ ] When a cascade Topic reaches scored, automatically approve and dispatch its platform article jobs; finish the run after all target articles reach reviewable state.
- [ ] Serve run history as JSON and stream new events using `Last-Event-ID` plus sequence polling.

### Task 3: Propagate worker execution events

**Files:** `src/scholar_agents/worker/consumer.py`, worker tests.

- [ ] Emit node start/completion/failure events for jobs carrying `workflowRunId`.
- [ ] Register created raw items, Topics, and articles as artifacts using the existing correlation context.
- [ ] Add tests for workflow metadata propagation and event payload shape.

### Task 4: Build the visual homepage

**Files:** `src/app/page.tsx`, `src/components/workflow/*`, `src/lib/api.ts`, `src/lib/workflow-events.ts`, `src/app/globals.css`, `package.json`.

- [ ] Add `@xyflow/react` and render the fixed source → Topic → article DAG with real statuses and animated active edges.
- [ ] Add run selector, cascade trigger, node inspector, artifact links, timeline, loading, empty, error, and reconnect states.
- [ ] Keep existing navigation targets for detailed topic/article/source review.

### Task 5: Verify and deploy

**Files:** affected repositories only.

- [ ] Run Go tests/build, Python tests, shared contract validation, frontend lint/build, and `git diff --check`.
- [ ] Apply the migration, restart Core/Agents/client processes, and verify manual trigger plus SSE against `https://scholar.aicave.cn`.
- [ ] Commit and push each repository without reverting unrelated existing work.
