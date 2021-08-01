package bs

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// GetProto reads a blob from a blob store and parses it into the given protocol buffer.
func GetProto(ctx context.Context, g Getter, ref Ref, m proto.Message) error {
	b, err := g.Get(ctx, ref)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, m)
}

// PutProto serializes m and stores it as a blob in s.
//
// The boolean result tells whether m's blob was newly added.
func PutProto(ctx context.Context, s Store, m proto.Message) (Ref, bool, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, false, errors.Wrap(err, "marshaling protobuf")
	}

	return s.Put(ctx, b)
}

// ProtoRef is a convenience function for computing the ref of a serialized protobuf.
func ProtoRef(m proto.Message) (Ref, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, err
	}
	return Blob(b).Ref(), nil
}
