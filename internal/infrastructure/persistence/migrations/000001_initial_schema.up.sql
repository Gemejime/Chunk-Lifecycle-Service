-- 000001_initial_schema.up.sql
-- Production schema for rag-chunk-lifecycle-service

-- 1. High-Velocity Unlogged Staging Buffer (for COPY / unnest high-throughput ingestion)
CREATE UNLOGGED TABLE IF NOT EXISTS unlogged_chunk_staging (
    batch_id UUID NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    chunk_id CHAR(64) NOT NULL,
    parent_id VARCHAR(128) NOT NULL DEFAULT '',
    content TEXT NOT NULL,
    token_count INT NOT NULL,
    structural_path VARCHAR(512) NOT NULL,
    byte_offset_start BIGINT NOT NULL,
    byte_offset_end BIGINT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_unlogged_chunk_staging_batch_id 
    ON unlogged_chunk_staging (batch_id, tenant_id);

-- 2. Document Manifests (Monotonic version aggregate root)
CREATE TABLE IF NOT EXISTS document_manifests (
    tenant_id VARCHAR(128) NOT NULL,
    document_id VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    chunk_count INT NOT NULL DEFAULT 0,
    total_tokens INT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, document_id, version)
);

CREATE INDEX IF NOT EXISTS idx_document_manifests_latest 
    ON document_manifests (tenant_id, document_id, version DESC);

-- 3. Content-Addressed Chunk Storage (Immutable nodes)
CREATE TABLE IF NOT EXISTS chunk_nodes (
    tenant_id VARCHAR(128) NOT NULL,
    chunk_id CHAR(64) NOT NULL,
    parent_id VARCHAR(128) NOT NULL DEFAULT '',
    content TEXT NOT NULL,
    token_count INT NOT NULL,
    structural_path VARCHAR(512) NOT NULL,
    byte_offset_start BIGINT NOT NULL,
    byte_offset_end BIGINT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, chunk_id)
);

CREATE INDEX IF NOT EXISTS idx_chunk_nodes_parent 
    ON chunk_nodes (tenant_id, parent_id);

-- 4. Manifest-Chunk Junction Bindings (Content-addressed deduplication across versions)
CREATE TABLE IF NOT EXISTS manifest_chunk_bindings (
    tenant_id VARCHAR(128) NOT NULL,
    document_id VARCHAR(128) NOT NULL,
    manifest_version BIGINT NOT NULL,
    chunk_id CHAR(64) NOT NULL,
    ordinal_position INT NOT NULL,
    PRIMARY KEY (tenant_id, document_id, manifest_version, ordinal_position),
    FOREIGN KEY (tenant_id, chunk_id) REFERENCES chunk_nodes (tenant_id, chunk_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_manifest_chunk_bindings_chunk 
    ON manifest_chunk_bindings (tenant_id, chunk_id);

-- 5. Parent Containers (Compiled context windows)
CREATE TABLE IF NOT EXISTS parent_containers (
    tenant_id VARCHAR(128) NOT NULL,
    parent_id VARCHAR(128) NOT NULL,
    document_id VARCHAR(128) NOT NULL,
    manifest_version BIGINT NOT NULL,
    heading_path VARCHAR(512) NOT NULL,
    content TEXT NOT NULL,
    token_count INT NOT NULL,
    child_chunk_ids TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, parent_id, manifest_version)
);

CREATE INDEX IF NOT EXISTS idx_parent_containers_lookup 
    ON parent_containers (tenant_id, document_id, manifest_version);

-- 6. Section Patches (Operator override ledger & quarantine state machine)
CREATE TABLE IF NOT EXISTS section_patches (
    tenant_id VARCHAR(128) NOT NULL,
    patch_id UUID NOT NULL,
    document_id VARCHAR(128) NOT NULL,
    section_key VARCHAR(512) NOT NULL,
    base_content_hash CHAR(64) NOT NULL,
    patched_content TEXT NOT NULL,
    operator_id VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, patch_id)
);

CREATE INDEX IF NOT EXISTS idx_section_patches_active 
    ON section_patches (tenant_id, document_id, section_key, status);

CREATE INDEX IF NOT EXISTS idx_section_patches_quarantine 
    ON section_patches (tenant_id, status, created_at DESC);

-- 7. Transactional Outbox (WAL tailing for Debezium CDC -> Kafka)
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id UUID PRIMARY KEY,
    tenant_id VARCHAR(128) NOT NULL,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload_json JSONB NOT NULL,
    trigger_reason VARCHAR(64) NOT NULL,
    timestamp_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished 
    ON outbox_events (published_at, timestamp_utc) 
    WHERE published_at IS NULL;
