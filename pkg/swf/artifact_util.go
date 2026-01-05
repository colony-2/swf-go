package swf

import (
	"crypto/sha256"
	"fmt"
	"io"
)

// computeSha256 computes the SHA256 hash of data from a reader
func computeSha256(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("compute sha256: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
