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

func (r Ref) Less(other Ref) bool {
	return bytes.Compare(r[:], other[:]) < 0
}

func (r Ref) FromHex(s string) error {
	if len(s) != 2*sha256.Size {
		return errors.New("wrong length")
	}
	_, err := hex.Decode(r[:], []byte(s))
	return err
}

func RefFromBytes(b []byte) Ref {
	var out Ref
	copy(out[:], b)
	return out
}

func RefFromHex(s string) (Ref, error) {
	var out Ref
	err := out.FromHex(s)
	return out, err
}
