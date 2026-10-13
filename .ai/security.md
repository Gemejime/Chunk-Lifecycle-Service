# Security Context — rag-chunk-lifecycle-service

## 1. Threat Model & Trust Boundaries

```
[ Client / Orchestrator ] (Untrusted / External TLS)
          │
          ▼
┌────────────────────────────────────────────────────────┐
│  Ingress Boundary: rag-chunk-lifecycle-service        │
│  • Token-Bucket Rate Limiting (RPM / TPM)              │
│  • Tenant Authentication & Authorization Interceptors │
│  • Schema & Token Budget Input Validation             │
│  • SECURED LOGGING (Redacted JSON with Trace IDs)      │
└──────────────┬──────────────────────────┬──────────────┘
               │                          │
               ▼                          ▼
   ┌───────────────────────┐  ┌───────────────────────┐
   │ PostgreSQL 16 DB      │  │ Redis 7.2 Cluster     │
   │ (PgBouncer, Parameter-│  │ (Versioned Tenant     │
   │  ized Queries, RLS)   │  │  Isolated Keys)       │
   └───────────────────────┘  └───────────────────────┘
```

- **Ingress Plane (gRPC / HTTP)**: Semi-trusted. Must validate caller identity, enforce tenant-level quotas via token-bucket algorithms, and validate all message payloads against strict Protobuf and OpenAPI schemas.
- **Tenant Boundaries**: Hard isolation. No operation can access or mutate chunks, manifests, or parent containers across tenant boundaries. Every query and cache key includes `tenant_id`.
- **Database & Persistence Layer**: Accessible only via PgBouncer with parameterization (`default_query_exec_mode = exec`). Raw string SQL concatenation is strictly forbidden.
- **Outbox & Event Egress**: Internal network only. Outbox events emitted to Kafka/Triton contain only non-confidential IDs and re-vectorization triggers.

---

## 2. Secured Logging Invariants & Enforcement

> [!CAUTION]
> **Zero Sensitive Data Leakage in Logs**:
> Log emission is a primary vector for unintended data exfiltration. Every logging statement must pass strict redaction rules.

### Secured Logging Rules:
1. **Redaction of Authentication Credentials**:
   - Strip or mask all Bearer tokens, JWT signatures, API keys, passwords, and connection strings.
   - Example sanitized log entry:
     ```json
     {"level":"info","ts":"2026-10-13T04:00:00Z","msg":"authenticated request","tenant_id":"ten-4912","auth_type":"bearer","token_prefix":"eyJhbG...[REDACTED]"}
     ```
2. **Chunk Text & PII / PHI Masking**:
   - **Never log raw document bodies or prompt text.**
   - Ingestion logs must record only deterministic identifiers and metrics:
     - `chunk_id`: `a8f9c4...`
     - `content_sha256`: `d2a1b9...`
     - `token_count`: `184`
     - `structural_path`: `doc_uuid/sec_4/para_2`
   - If payload previews are required for debugging in staging, enforce a hard limit of 32 runes of normalized text with PII/PHI scrubbing.
3. **Sanitized Error Logs & Internal Details**:
   - Internal stack traces, raw SQL queries, and database connection strings containing credentials must be scrubbed before logging.
   - Client-facing error responses must return generic error messages with a tracking `trace_id` (e.g. `{"error":"internal processing error","trace_id":"trc-9821"}`), never raw system errors.

---

## 3. Tenant Boundary & Data Protection Invariants

1. **Deterministic Cryptographic Hashing**:
   - Chunk identifiers are computed using standard cryptographic SHA-256 over canonical 8-byte big-endian length-prefixed UTF-8 encoded fields:
     $$\text{ChunkID} = \text{SHA256}(\text{len}_{8\text{BE}}(\text{TenantID}) \parallel \text{TenantID} \parallel \text{len}_{8\text{BE}}(\text{NormalizedContent}) \parallel \text{NormalizedContent} \parallel \text{len}_{8\text{BE}}(\text{StructuralPath}) \parallel \text{StructuralPath})$$
   - Length-prefixing prevents delimiter/field collision attacks, while hashing provides tamper-evident isolation across tenant namespaces.
2. **Strict Multi-Tenant Scoping**:
   - Every database query in `internal/infrastructure/persistence/postgres/` must filter by `tenant_id`.
   - Every Redis cache key must be partitioned by tenant: `chunk:parent:{tenant_id}:{parent_id}:v{version}`.
3. **Idempotency & Replay Protection**:
   - Ingestion stream records are idempotent by hash.
   - Patch mutations require client idempotency tokens to prevent duplicate modifications under network retries.

---

## 4. Configuration & Secrets Management

- **12-Factor App Compliance**: All configuration (database credentials, Redis hosts, Kafka topics) is supplied via environment variables.
- **Zero Secrets in Git**:
  - `.env*` files, private keys (`*.key`, `*.pem`), and certificates are strictly excluded in `.gitignore`.
  - Secret scanning (e.g. `gitleaks` or `trufflehog`) is a mandatory security gate, to be enforced on staged changes in pre-commit and independently in CI runs once configured.

---

## 5. Container & Runtime Security

- **Distroless Base Image**: Production containers use `gcr.io/distroless/static:nonroot`.
- **Zero Shell Environment**: No `/bin/sh`, `bash`, or package managers in the production image, eliminating post-exploitation shell spawns.
- **Non-Root Execution**: Runs under static non-root user (UID 65532).
- **Read-Only Root Filesystem**: Kubernetes pods run with `readOnlyRootFilesystem: true` with ephemeral memory-backed `/tmp` if needed.

---

## 6. Pre-Commit Security Audit Checklist

Before submitting or committing any change:
1. [ ] **Secured Logging**: Verify all new `slog`/`zap` statements do not output raw document text, tokens, or PII.
2. [ ] **Doc API Invariant**: Ensure API documentation (`api/openapi/openapi.yaml`, Protobuf comments) is updated to reflect all contract changes.
3. [ ] **SQL Injection Prevention**: Verify all SQL queries use parameterized arguments (`$1, $2, ...`).
4. [ ] **Secret Scan**: Verify no credentials, tokens, or `.env` files are staged.
5. [ ] **Dependency Audit**: Run `govulncheck ./...` to verify zero known vulnerabilities.
