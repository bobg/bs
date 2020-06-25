// Package bs describes a content-addressable blob store.
package bs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type (
	// Blob is the type of a blob.
	Blob []byte

	// Ref is a reference of a blob: its sha256 hash.
	Ref [sha256.Size]byte

	// Anchor maps an arbitrary string to one or more blob references.
	Anchor string
)

// Ref computes the Ref of a blob.
func (b Blob) Ref() Ref {
	return sha256.Sum256(b)
}

// String converts a Ref to hexadecimal.
func (r Ref) String() string {
	return hex.EncodeToString(r[:])
}

// Less tells whether `r` is lexicographically less than `other`.
func (r Ref) Less(other Ref) bool {
	return bytes.Compare(r[:], other[:]) < 0
}

// FromHex parses the hex string `s` and places the result in `r`.
func (r *Ref) FromHex(s string) error {
	if len(s) != 2*sha256.Size {
		return errors.New("wrong length")
	}
	_, err := hex.Decode(r[:], []byte(s))
	return err
}

// RefFromBytes produces a Ref from a byte slice.
func RefFromBytes(b []byte) Ref {
	var out Ref
	copy(out[:], b)
	return out
}

// RefFromHex produces a Ref from a hex string.
func RefFromHex(s string) (Ref, error) {
	var out Ref
	err := out.FromHex(s)
	return out, err
}
