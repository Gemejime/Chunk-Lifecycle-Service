package entity

import "errors"

var (
	// ErrEmptyTenantID is returned when tenant_id is empty or missing.
	ErrEmptyTenantID = errors.New("tenant_id must not be empty")

	// ErrEmptyDocumentID is returned when document_id is empty or missing.
	ErrEmptyDocumentID = errors.New("document_id must not be empty")

	// ErrEmptyChunkID is returned when chunk_id is empty or missing.
	ErrEmptyChunkID = errors.New("chunk_id must not be empty")

	// ErrInvalidChunkID is returned when chunk_id does not conform to 64-char lowercase hex.
	ErrInvalidChunkID = errors.New("chunk_id must be a 64-character lowercase hexadecimal SHA-256 hash")

	// ErrEmptyParentID is returned when parent_id is empty or missing.
	ErrEmptyParentID = errors.New("parent_id must not be empty")

	// ErrTokenBudgetViolation is returned when token count violates child or parent bounds.
	ErrTokenBudgetViolation = errors.New("token count violates prescribed budget bounds")

	// ErrInvalidStructuralPath is returned when structural path is empty or malformed.
	ErrInvalidStructuralPath = errors.New("structural_path must not be empty")

	// ErrInvalidByteOffsets is returned when byte offset bounds are negative or inverted.
	ErrInvalidByteOffsets = errors.New("byte offsets must be non-negative with start <= end")

	// ErrVersionRegression is returned when an attempted manifest version is not strictly monotonic (v_n+1 <= v_n).
	ErrVersionRegression = errors.New("manifest version must strictly advance monotonically")

	// ErrManifestAlreadyFinalized is returned when attempting to mutate a finalized document manifest.
	ErrManifestAlreadyFinalized = errors.New("document manifest version is finalized and immutable")

	// ErrInvalidPatchStatusTransition is returned when a patch state machine transition is illegal.
	ErrInvalidPatchStatusTransition = errors.New("illegal section patch status transition")

	// ErrEmptySectionKey is returned when section_key is empty or missing.
	ErrEmptySectionKey = errors.New("section_key must not be empty")

	// ErrEmptyBaseContentHash is returned when base_content_hash is empty or missing.
	ErrEmptyBaseContentHash = errors.New("base_content_hash must not be empty")

	// ErrEmptyOperatorID is returned when operator_id is empty or missing.
	ErrEmptyOperatorID = errors.New("operator_id must not be empty")

	// ErrStaleBaseHash is returned when the operator patch base_content_hash does not match the active section content hash.
	ErrStaleBaseHash = errors.New("base content hash does not match active section content")
)
