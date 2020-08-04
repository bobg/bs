package bs

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// PutProto adds the serialization of a protocol buffer as a single blob to a blob store.
func PutProto(ctx context.Context, s Store, p proto.Message) (ref Ref, added bool, err error) {
	m, err := proto.Marshal(p)
	if err != nil {
		return Ref{}, false, err
	}
	return s.Put(ctx, m)
}

// GetProto reads a blob from a blob store and parses it into the given protocol buffer.
func GetProto(ctx context.Context, g Getter, ref Ref, p proto.Message) error {
	b, err := g.Get(ctx, ref)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, p)
}

// ProtoRef computes the ref of a protocol buffer (by marshaling it as a blob).
func ProtoRef(p proto.Message) (Ref, error) {
	m, err := proto.Marshal(p)
	if err != nil {
		return Ref{}, err
	}
	return Blob(m).Ref(), nil
}
