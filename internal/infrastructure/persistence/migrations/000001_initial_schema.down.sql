-- 000001_initial_schema.down.sql
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS section_patches;
DROP TABLE IF EXISTS parent_containers;
DROP TABLE IF EXISTS manifest_chunk_bindings;
DROP TABLE IF EXISTS chunk_nodes;
DROP TABLE IF EXISTS document_manifests;
DROP TABLE IF EXISTS unlogged_chunk_staging;
