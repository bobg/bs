package bs

import (
	"bytes"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
)

type (
	// Blob is the type of a blob.
	Blob []byte

	// Ref is the reference of a blob: its sha256 hash.
	Ref [sha256.Size]byte
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
// The length of the byte slice is not checked.
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

// Value implements the Valuer interface for database/sql.
func (r Ref) Value() (driver.Value, error) {
	return r[:], nil
}

// Scan implements the Scanner interface for database/sql.
func (r *Ref) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	if b, ok := src.([]byte); ok {
		copy((*r)[:], b)
		return nil
	}
	return fmt.Errorf("cannot scan %T into *Ref", src)
}
