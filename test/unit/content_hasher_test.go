package unit

import (
	"testing"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/compiler"
)

func TestContentHasher_DeterministicHashing(t *testing.T) {
	hasher := compiler.NewContentHasher()
	tenantID := "tenant-test-1"
	content := "Vector indexing and content addressing ensure data plane reproducibility."
	path := "doc-100/sec_1/para_1"

	hash1 := hasher.ComputeChunkID(tenantID, content, path)
	hash2 := hasher.ComputeChunkID(tenantID, content, path)

	if hash1 != hash2 {
		t.Fatalf("Expected deterministic hashes to match, got %s and %s", hash1, hash2)
	}
	if len(hash1) != 64 {
		t.Fatalf("Expected 64-char hex hash, got length %d (%s)", len(hash1), hash1)
	}
}

func TestContentHasher_UnicodeNFCStability(t *testing.T) {
	hasher := compiler.NewContentHasher()
	tenantID := "tenant-test-1"
	path := "doc-100/sec_1/para_1"

	// Precomposed 'é' (\u00e9) vs decomposed combining character 'e\u0301'
	composed := "Café culture and résumé processing"
	decomposed := "Cafe\u0301 culture and re\u0301sume\u0301 processing"

	hashComposed := hasher.ComputeChunkID(tenantID, composed, path)
	hashDecomposed := hasher.ComputeChunkID(tenantID, decomposed, path)

	if hashComposed != hashDecomposed {
		t.Fatalf("Unicode NFC normalization failed: composed %s != decomposed %s", hashComposed, hashDecomposed)
	}
}

func TestContentHasher_CRLFNormalization(t *testing.T) {
	hasher := compiler.NewContentHasher()
	tenantID := "tenant-test-1"
	path := "doc-100/sec_1/para_1"

	contentLF := "Line 1\nLine 2\nLine 3"
	contentCRLF := "Line 1\r\nLine 2\r\nLine 3"
	contentCR := "Line 1\rLine 2\rLine 3"

	hashLF := hasher.ComputeChunkID(tenantID, contentLF, path)
	hashCRLF := hasher.ComputeChunkID(tenantID, contentCRLF, path)
	hashCR := hasher.ComputeChunkID(tenantID, contentCR, path)

	if hashLF != hashCRLF {
		t.Fatalf("Line ending normalization failed: LF %s != CRLF %s", hashLF, hashCRLF)
	}
	if hashLF != hashCR {
		t.Fatalf("Line ending normalization failed: LF %s != CR %s", hashLF, hashCR)
	}
}

func TestContentHasher_WhitespaceCollapse(t *testing.T) {
	hasher := compiler.NewContentHasher()
	tenantID := "tenant-test-1"
	path := "doc-100/sec_1/para_1"

	clean := "Paragraph with single spaces."
	messy := "   Paragraph   with \t \t single   spaces.   "

	hashClean := hasher.ComputeChunkID(tenantID, clean, path)
	hashMessy := hasher.ComputeChunkID(tenantID, messy, path)

	if hashClean != hashMessy {
		t.Fatalf("Whitespace collapsing failed: clean %s != messy %s", hashClean, hashMessy)
	}
}

func TestContentHasher_CollisionResistance_LengthPrefix(t *testing.T) {
	hasher := compiler.NewContentHasher()

	// Delimiter collision test:
	// Tenant "ab", Content "c" vs Tenant "a", Content "bc" with identical paths
	path := "sec_1"
	hash1 := hasher.ComputeChunkID("ab", "c", path)
	hash2 := hasher.ComputeChunkID("a", "bc", path)

	if hash1 == hash2 {
		t.Fatalf("Length prefix failed to prevent delimiter collision: hash1 == hash2 (%s)", hash1)
	}

	// Content "x", Path "yz" vs Content "xy", Path "z"
	tenant := "ten_1"
	hash3 := hasher.ComputeChunkID(tenant, "x", "yz")
	hash4 := hasher.ComputeChunkID(tenant, "xy", "z")

	if hash3 == hash4 {
		t.Fatalf("Length prefix failed to prevent path collision: hash3 == hash4 (%s)", hash3)
	}
}
