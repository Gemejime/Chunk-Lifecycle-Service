package entity

import (
	"fmt"
	"strings"
)

// MinParentTokenBudget defines the lower bound for compiled parent context windows.
const MinParentTokenBudget = 800

// MaxParentTokenBudget defines the upper bound for compiled parent context windows.
const MaxParentTokenBudget = 1500

// ParentContainer represents a compiled parent context window entity.
type ParentContainer struct {
	parentID        string
	tenantID        string
	documentID      string
	manifestVersion int64
	headingPath     string
	content         string
	tokenCount      int
	childChunkIDs   []string
}

// NewParentContainer constructs and validates a compiled parent context window.
// Invariants enforced:
// - Non-empty parentID, tenantID, and documentID.
// - Monotonic positive manifest version (> 0).
// - Token budget strictly bounded between 800 and 1,500 tokens.
func NewParentContainer(
	parentID string,
	tenantID string,
	documentID string,
	manifestVersion int64,
	headingPath string,
	content string,
	tokenCount int,
	childChunkIDs []string,
) (*ParentContainer, error) {
	parentID = strings.TrimSpace(parentID)
	if parentID == "" {
		return nil, ErrEmptyParentID
	}

	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return nil, ErrEmptyTenantID
	}

	documentID = strings.TrimSpace(documentID)
	if documentID == "" {
		return nil, ErrEmptyDocumentID
	}

	if manifestVersion <= 0 {
		return nil, fmt.Errorf("%w: version %d must be positive", ErrVersionRegression, manifestVersion)
	}

	if tokenCount < MinParentTokenBudget || tokenCount > MaxParentTokenBudget {
		return nil, fmt.Errorf("%w: parent container token count %d outside [%d, %d]",
			ErrTokenBudgetViolation, tokenCount, MinParentTokenBudget, MaxParentTokenBudget)
	}

	childrenCopy := make([]string, len(childChunkIDs))
	copy(childrenCopy, childChunkIDs)

	return &ParentContainer{
		parentID:        parentID,
		tenantID:        tenantID,
		documentID:      documentID,
		manifestVersion: manifestVersion,
		headingPath:     strings.TrimSpace(headingPath),
		content:         content,
		tokenCount:      tokenCount,
		childChunkIDs:   childrenCopy,
	}, nil
}

// ParentID returns the parent context container identifier.
func (p *ParentContainer) ParentID() string { return p.parentID }

// TenantID returns the tenant identifier.
func (p *ParentContainer) TenantID() string { return p.tenantID }

// DocumentID returns the document aggregate root identifier.
func (p *ParentContainer) DocumentID() string { return p.documentID }

// ManifestVersion returns the monotonic manifest version.
func (p *ParentContainer) ManifestVersion() int64 { return p.manifestVersion }

// HeadingPath returns the hierarchical heading breadcrumb path.
func (p *ParentContainer) HeadingPath() string { return p.headingPath }

// Content returns the compiled aggregated context text.
func (p *ParentContainer) Content() string { return p.content }

// TokenCount returns the token count within budget bounds [800, 1500].
func (p *ParentContainer) TokenCount() int { return p.tokenCount }

// ChildChunkIDs returns a copy of child chunk IDs bound to this window.
func (p *ParentContainer) ChildChunkIDs() []string {
	cp := make([]string, len(p.childChunkIDs))
	copy(cp, p.childChunkIDs)
	return cp
}
