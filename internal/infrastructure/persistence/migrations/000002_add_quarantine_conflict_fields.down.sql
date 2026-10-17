-- 000002_add_quarantine_conflict_fields.down.sql
-- Reverts upstream CDC conflict tracking columns and indexes

DROP INDEX IF EXISTS idx_section_patches_quarantine_keyset;
DROP INDEX IF EXISTS idx_section_patches_quarantine_lookup;

ALTER TABLE section_patches 
    DROP COLUMN IF EXISTS upstream_content,
    DROP COLUMN IF EXISTS upstream_content_hash,
    DROP COLUMN IF EXISTS conflict_detected_at;
