package bs

// Blob is a data blob.
type Blob interface {
	Bytes() []byte
}

// Bytes is a byte array implementing Blob.
type Bytes []byte

// Bytes implements Blob.
func (b Bytes) Bytes() []byte { return b }

// TBlob is a typed Blob.
type TBlob interface {
	Blob
	Type() []byte
}

// TBytes is a Blob plus a type implementing TBlob.
type TBytes struct {
	B Blob
	T []byte
}

func (t TBytes) Bytes() []byte { return t.B.Bytes() }
func (t TBytes) Type() []byte  { return t.T }
