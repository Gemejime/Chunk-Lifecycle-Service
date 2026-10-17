-- 000002_add_quarantine_conflict_fields.up.sql
-- Adds upstream CDC conflict tracking columns and indexes for section quarantine review

ALTER TABLE section_patches 
    ADD COLUMN IF NOT EXISTS upstream_content TEXT,
    ADD COLUMN IF NOT EXISTS upstream_content_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS conflict_detected_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_section_patches_quarantine_lookup 
    ON section_patches (tenant_id, patch_id);

CREATE INDEX IF NOT EXISTS idx_section_patches_quarantine_keyset 
    ON section_patches (tenant_id, status, created_at DESC, patch_id DESC);
