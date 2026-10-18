package compiler

import (
	"context"
	"fmt"
	"strings"

	"github.com/Gemejime/rag-chunk-lifecycle-service/internal/domain/entity"
)

// ExtractSectionKey extracts the section prefix from a hierarchical structural path.
// Examples:
// - "doc_uuid/sec_4/para_2" -> "doc_uuid/sec_4"
// - "doc_uuid/sec_4"        -> "doc_uuid/sec_4"
// - "single_segment"        -> "single_segment"
func ExtractSectionKey(structuralPath string) string {
	cleaned := strings.TrimSpace(structuralPath)
	parts := strings.Split(cleaned, "/")
	if len(parts) >= 2 {
		return parts[0] + "/" + parts[1]
	}
	return cleaned
}

// ContextCompiler compiles fine-grained child chunks (100–300 tokens) into coherent
// parent context containers (800–1,500 tokens), handling structural section boundary splits,
// tail window allowances (< 800 tokens at EOF or section boundary), and deterministic ADR-001 ParentIDs.
type ContextCompiler struct {
	hasher *ContentHasher
}

// NewContextCompiler constructs a new ContextCompiler instance.
func NewContextCompiler() *ContextCompiler {
	return &ContextCompiler{
		hasher: NewContentHasher(),
	}
}

// Compile compiles an ordered slice of ChunkNodes into parent context containers.
// Rules:
// 1. Validates non-empty tenantID, documentID, and positive manifestVersion.
// 2. Groups ordered chunks, accumulating tokens up to MaxParentTokenBudget (1,500).
// 3. Closes and emits a ParentContainer window when:
//   - The structural section key changes (e.g. sec_4 -> sec_5).
//   - Adding the next child would exceed MaxParentTokenBudget (1,500).
//   - The end of the chunk slice (EOF) is reached.
//
// 4. Tail remainder windows (< 800 tokens) are validly emitted at section boundaries and EOF.
// 5. Parent IDs are deterministically computed via ContentHasher adhering to ADR-001.
func (c *ContextCompiler) Compile(
	ctx context.Context,
	tenantID string,
	documentID string,
	manifestVersion int64,
	chunks []*entity.ChunkNode,
) ([]*entity.ParentContainer, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, entity.ErrEmptyTenantID
	}

	documentID = strings.TrimSpace(documentID)
	if documentID == "" {
		return nil, entity.ErrEmptyDocumentID
	}

	if manifestVersion <= 0 {
		return nil, fmt.Errorf("%w: version %d must be positive", entity.ErrVersionRegression, manifestVersion)
	}

	if len(chunks) == 0 {
		return []*entity.ParentContainer{}, nil
	}

	var containers []*entity.ParentContainer

	var currentSectionKey string
	var currentHeadingPath string
	var currentChildren []string
	var currentContents []string
	var currentTokenCount int

	flush := func() error {
		if len(currentChildren) == 0 {
			return nil
		}

		aggregatedContent := strings.Join(currentContents, "\n\n")
		parentID := c.hasher.ComputeParentID(tenantID, documentID, currentHeadingPath, currentChildren)

		container, err := entity.NewParentContainer(
			parentID,
			tenantID,
			documentID,
			manifestVersion,
			currentHeadingPath,
			aggregatedContent,
			currentTokenCount,
			currentChildren,
		)
		if err != nil {
			return fmt.Errorf("failed to construct parent container: %w", err)
		}

		containers = append(containers, container)

		// Reset accumulator
		currentSectionKey = ""
		currentHeadingPath = ""
		currentChildren = nil
		currentContents = nil
		currentTokenCount = 0
		return nil
	}

	for _, ch := range chunks {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		secKey := ExtractSectionKey(ch.StructuralPath())
		heading := ch.Metadata()["heading_path"]
		if heading == "" {
			heading = ch.Metadata()["heading"]
		}
		if heading == "" {
			heading = secKey
		}

		// Boundary conditions to flush current window:
		// 1. Structural section boundary crossed
		sectionChanged := len(currentChildren) > 0 && secKey != currentSectionKey
		// 2. Token ceiling: adding next child would exceed 1,500 tokens
		budgetExceeded := len(currentChildren) > 0 && (currentTokenCount+ch.TokenCount() > entity.MaxParentTokenBudget)

		if sectionChanged || budgetExceeded {
			if err := flush(); err != nil {
				return nil, err
			}
		}

		if len(currentChildren) == 0 {
			currentSectionKey = secKey
			currentHeadingPath = heading
		}

		currentChildren = append(currentChildren, ch.ChunkID())
		currentContents = append(currentContents, ch.Content())
		currentTokenCount += ch.TokenCount()
	}

	// Flush trailing window at EOF
	if err := flush(); err != nil {
		return nil, err
	}

	return containers, nil
}
