package swf

import (
	"errors"
	"strings"
	"testing"
)

func TestArtifactKeyUnavailableForNewArtifact(t *testing.T) {
	art := NewArtifactFromBytes("output.txt", []byte("hello"))
	_, err := art.ArtifactKey()
	if err == nil {
		t.Fatal("expected error for non-persisted artifact key")
	}
	if !errors.Is(err, ErrArtifactKeyUnavailable) {
		t.Fatalf("expected ErrArtifactKeyUnavailable, got %v", err)
	}
	if !strings.Contains(err.Error(), "persisted") {
		t.Fatalf("expected error to mention persistence, got %q", err.Error())
	}
}
