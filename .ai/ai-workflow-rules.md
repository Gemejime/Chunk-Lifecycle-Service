# AI Workflow Rules — rag-chunk-lifecycle-service

## 1. Development Approach

This microservice is built incrementally using a **spec-driven, test-anchored workflow**.
All architectural and implementation decisions derive from:
1. The tracked specification manifests in `.ai/` (`project-overview.md`, `architecture.md`, `code-standards.md`, `security.md`, `progress-tracker.md`)
2. Local architecture and implementation blueprints in `tasks/` when present (`tasks/RAG Chunk Lifecycle Service Implementation Plan.pdf`, `tasks/Production Grade Backend Concepts.pdf`)

Never invent behaviors, schemas, or RPC signatures outside these specifications without explicit user confirmation.

---

## 2. Human-in-the-Loop Protocol

1. **Prompt-Planning Phase (`prompts/`)**:
   - For any non-trivial implementation, create an implementation plan/prompt file in `prompts/<task-name>.md`.
   - Ask for confirmation: `I prepared the implementation prompt at prompts/<task-name>.md. Is this good to execute?`
   - Only proceed with code authoring upon user approval.
2. **Ambiguity Escalation**:
   - Stop and ask when encountering architectural ambiguity, conflicting requirements, or unbudgeted scope.
3. **Destructive Action Safeguards**:
   - Require explicit human consent before running database schema drops, Git force pushes, or deleting tracked files.
4. **Verification & Proof**:
   - Provide concrete proof of correctness (passing tests, lint logs, curl/grpcurl invocations) upon completing any unit.

---

## 3. Non-Negotiable Invariants

1. **Scope Boundary**: Never implement out-of-scope features (e.g. 3-way AST token diffing, full text OCR, model weights hosting).
2. **Doc API Mandate**:
   > **The "doc api" must ALWAYS be updated no matter how little the created or modified code is.**
   > Every addition or alteration to externally exposed gRPC RPCs, Protobuf messages, HTTP routes, or OpenAPI endpoints must be documented in `api/openapi/openapi.yaml` and Protobuf comments in the exact same commit. Internal repository port contracts must be documented directly at their Go interfaces.
3. **Secured Logging Mandate**:
   > **All logging must be strictly secured.**
   > Output structured JSON with `trace_id` and `tenant_id`. Redact all authorization tokens, credentials, and raw document chunk contents (log only safe hashes, chunk IDs, and token metrics).
4. **Resilient Error Handling**:
   > Errors must be wrapped with domain context internally, while external client responses are sanitized without leaking internal stack traces or database schema details.
5. **Testing Integrity**:
   > Maintain tests across dedicated directories: `test/unit/`, `test/integration/`, and `test/performance/`. Tests must pass with the `-race` detector enabled.

---

## 4. Scoping & Slicing Rules

- Implement one discrete architectural component at a time (e.g., Domain Entity $\to$ Hasher Compiler $\to$ Migration DDL $\to$ Repository Adapter $\to$ Transport Handler).
- Never bundle unrelated layers (e.g., changing database migration scripts and gRPC transport interceptors simultaneously) unless executing an approved end-to-end slice.
- If a change cannot be verified locally via unit or integration tests, the scope is too broad — slice it.

---

## 5. Pre-Commit Verification Gate

Before committing code, opening a PR, or declaring a task complete:

1. **Format & Imports**: Run `goimports -w .` on all modified Go files.
2. **Static Analysis**: Run `golangci-lint run --timeout=5m` and ensure 0 errors.
3. **Test Suite**: Run `go test -v -race ./test/unit/...`. If database or cache code was modified, run integration tests: `go test -v -race -tags=integration ./test/integration/...`.
4. **Security & Vulnerability Check**: Run `govulncheck ./...` and audit logs for secured logging compliance.
5. **API Documentation Check**: Verify that `api/openapi/openapi.yaml` and Protobuf comments match the code changes exactly.
6. **Progress Tracker**: Update `.ai/progress-tracker.md` with completed items and current phase status.

---

## 6. Git Commit Standards
- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup

