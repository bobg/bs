// Package bs describes a content-addressable blob store.
package bs

import (
	"crypto/sha256"
	"encoding/hex"
)

type (
	// Blob is the type of a blob.
	Blob []byte

	// Ref is the ref of a blob: its sha256 hash.
	Ref [sha256.Size]byte

	Anchor string
)

// Ref computes the Ref of a blob.
func (b Blob) Ref() Ref {
	return sha256.Sum256(b)
}

// Zero is the zero value of a Ref.
var Zero Ref

func (r Ref) String() string {
	return hex.EncodeToString(r[:])
}
