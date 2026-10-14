package compiler

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"regexp"
	"strings"

	"golang.org/x/text/unicode/norm"
)

var horizontalWhitespaceRegex = regexp.MustCompile(`[ \t]+`)

// ContentHasher computes deterministic, content-addressed SHA-256 chunk IDs adhering to ADR-001.
type ContentHasher struct{}

// NewContentHasher creates a new ContentHasher instance.
func NewContentHasher() *ContentHasher {
	return &ContentHasher{}
}

// NormalizeContent applies the canonical text normalization pipeline:
// 1. Unicode NFC normalization (composing combining characters).
// 2. Line ending normalization (\r\n and \r to \n).
// 3. Horizontal whitespace collapsing (multiple spaces and tabs collapsed to a single space).
// 4. Per-line whitespace trimming and leading/trailing content trimming.
func (h *ContentHasher) NormalizeContent(raw string) string {
	// 1. Unicode NFC normalization
	nfc := norm.NFC.String(raw)

	// 2. Line ending normalization
	nfc = strings.ReplaceAll(nfc, "\r\n", "\n")
	nfc = strings.ReplaceAll(nfc, "\r", "\n")

	// 3. Process line-by-line: collapse internal spaces/tabs and trim line boundaries
	lines := strings.Split(nfc, "\n")
	for i, line := range lines {
		collapsed := horizontalWhitespaceRegex.ReplaceAllString(line, " ")
		lines[i] = strings.Trim(collapsed, " \t")
	}

	normalized := strings.Join(lines, "\n")
	return strings.TrimSpace(normalized)
}

// ComputeChunkID calculates the deterministic content-addressed Chunk ID according to ADR-001:
// ChunkID = SHA256(len_8BE(TenantID) || TenantID || len_8BE(NormalizedContent) || NormalizedContent || len_8BE(StructuralPath) || StructuralPath)
// Returns a lowercase 64-character hexadecimal string.
func (h *ContentHasher) ComputeChunkID(tenantID, rawContent, structuralPath string) string {
	normalizedContent := h.NormalizeContent(rawContent)
	return h.ComputeChunkIDWithNormalized(tenantID, normalizedContent, structuralPath)
}

// ComputeChunkIDWithNormalized calculates the Chunk ID assuming content has already been normalized.
func (h *ContentHasher) ComputeChunkIDWithNormalized(tenantID, normalizedContent, structuralPath string) string {
	tBytes := []byte(tenantID)
	cBytes := []byte(normalizedContent)
	pBytes := []byte(structuralPath)

	hasher := sha256.New()

	var lenBuf [8]byte

	// 1. len_8BE(TenantID) || TenantID
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(tBytes)))
	hasher.Write(lenBuf[:])
	hasher.Write(tBytes)

	// 2. len_8BE(NormalizedContent) || NormalizedContent
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(cBytes)))
	hasher.Write(lenBuf[:])
	hasher.Write(cBytes)

	// 3. len_8BE(StructuralPath) || StructuralPath
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(pBytes)))
	hasher.Write(lenBuf[:])
	hasher.Write(pBytes)

	digest := hasher.Sum(nil)
	return hex.EncodeToString(digest)
}

// ComputeParentID calculates the deterministic content-addressed Parent ID according to ADR-001:
// ParentID = SHA256(len_8BE(TenantID) || TenantID || len_8BE(DocumentID) || DocumentID || len_8BE(HeadingPath) || HeadingPath || len_8BE(CanonicalChildIDs) || CanonicalChildIDs)
// Returns a lowercase 64-character hexadecimal string.
func (h *ContentHasher) ComputeParentID(tenantID, documentID, headingPath string, childChunkIDs []string) string {
	canonicalChildren := strings.Join(childChunkIDs, ",")
	tBytes := []byte(tenantID)
	dBytes := []byte(documentID)
	hBytes := []byte(headingPath)
	cBytes := []byte(canonicalChildren)

	hasher := sha256.New()
	var lenBuf [8]byte

	// 1. len_8BE(TenantID) || TenantID
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(tBytes)))
	hasher.Write(lenBuf[:])
	hasher.Write(tBytes)

	// 2. len_8BE(DocumentID) || DocumentID
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(dBytes)))
	hasher.Write(lenBuf[:])
	hasher.Write(dBytes)

	// 3. len_8BE(HeadingPath) || HeadingPath
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(hBytes)))
	hasher.Write(lenBuf[:])
	hasher.Write(hBytes)

	// 4. len_8BE(CanonicalChildIDs) || CanonicalChildIDs
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(cBytes)))
	hasher.Write(lenBuf[:])
	hasher.Write(cBytes)

	digest := hasher.Sum(nil)
	return hex.EncodeToString(digest)
}
