package entity

import (
	"fmt"
	"regexp"
	"strings"
)

var hex64Regex = regexp.MustCompile(`^[0-9a-f]{64}$`)

// MinChunkTokenBudget defines the lower bound for fine-grained child chunks.
const MinChunkTokenBudget = 100

// MaxChunkTokenBudget defines the upper bound for fine-grained child chunks.
const MaxChunkTokenBudget = 300

// ChunkNode represents an immutable, content-addressed child chunk node.
type ChunkNode struct {
	chunkID         string
	tenantID        string
	parentID        string
	content         string
	tokenCount      int
	structuralPath  string
	byteOffsetStart int64
	byteOffsetEnd   int64
	metadata        map[string]string
}

// NewChunkNode constructs and validates a new immutable ChunkNode.
// Invariants enforced:
// - Non-empty tenantID and structuralPath.
// - Deterministic 64-character lowercase hex ChunkID.
// - Token budget strictly bounded between 100 and 300 tokens.
// - Non-negative, ordered byte offsets.
func NewChunkNode(
	chunkID string,
	tenantID string,
	parentID string,
	content string,
	tokenCount int,
	structuralPath string,
	byteOffsetStart int64,
	byteOffsetEnd int64,
	metadata map[string]string,
) (*ChunkNode, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, ErrEmptyTenantID
	}

	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil, ErrEmptyChunkID
	}
	if !hex64Regex.MatchString(chunkID) {
		return nil, fmt.Errorf("%w: got %q", ErrInvalidChunkID, chunkID)
	}

	if tokenCount < MinChunkTokenBudget || tokenCount > MaxChunkTokenBudget {
		return nil, fmt.Errorf("%w: chunk token count %d outside [%d, %d]",
			ErrTokenBudgetViolation, tokenCount, MinChunkTokenBudget, MaxChunkTokenBudget)
	}

	structuralPath = strings.TrimSpace(structuralPath)
	if structuralPath == "" {
		return nil, ErrInvalidStructuralPath
	}

	if byteOffsetStart < 0 || byteOffsetEnd < byteOffsetStart {
		return nil, fmt.Errorf("%w: start=%d, end=%d", ErrInvalidByteOffsets, byteOffsetStart, byteOffsetEnd)
	}

	metaCopy := make(map[string]string, len(metadata))
	for k, v := range metadata {
		metaCopy[k] = v
	}

	return &ChunkNode{
		chunkID:         chunkID,
		tenantID:        tenantID,
		parentID:        strings.TrimSpace(parentID),
		content:         content,
		tokenCount:      tokenCount,
		structuralPath:  structuralPath,
		byteOffsetStart: byteOffsetStart,
		byteOffsetEnd:   byteOffsetEnd,
		metadata:        metaCopy,
	}, nil
}

// ChunkID returns the content-addressed lowercase SHA-256 hash.
func (c *ChunkNode) ChunkID() string { return c.chunkID }

// TenantID returns the tenant identifier.
func (c *ChunkNode) TenantID() string { return c.tenantID }

// ParentID returns the enclosing parent context container identifier.
func (c *ChunkNode) ParentID() string { return c.parentID }

// Content returns the normalized chunk text.
func (c *ChunkNode) Content() string { return c.content }

// TokenCount returns the token count within budget bounds [100, 300].
func (c *ChunkNode) TokenCount() int { return c.tokenCount }

// StructuralPath returns the hierarchical breadcrumb path.
func (c *ChunkNode) StructuralPath() string { return c.structuralPath }

// ByteOffsetStart returns the 0-indexed starting byte offset in source document.
func (c *ChunkNode) ByteOffsetStart() int64 { return c.byteOffsetStart }

// ByteOffsetEnd returns the 0-indexed ending byte offset in source document.
func (c *ChunkNode) ByteOffsetEnd() int64 { return c.byteOffsetEnd }

// Metadata returns a defensive copy of associated key-value attributes.
func (c *ChunkNode) Metadata() map[string]string {
	cp := make(map[string]string, len(c.metadata))
	for k, v := range c.metadata {
		cp[k] = v
	}
	return cp
}
