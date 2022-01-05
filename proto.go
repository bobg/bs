package bs

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
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
		return Zero, false, errors.Wrap(err, "marshaling protobuf")
	}

	ref, added, err := s.Put(ctx, Bytes(b))
	if err != nil {
		return Zero, false, errors.Wrap(err, "storing marshaled protobuf")
	}

	if ts, ok := s.(TStore); ok {
		protoDescriptor := m.ProtoReflect().Descriptor()
		protoType := protodesc.ToDescriptorProto(protoDescriptor)
		typ, err := proto.Marshal(protoType)
		if err != nil {
			return Zero, false, errors.Wrap(err, "marshing protobuf type")
		}
		err = ts.PutType(ctx, ref, typ)
		if err != nil {
			return ref, added, errors.Wrap(err, "storing type")
		}
	}

	return ref, added, nil
}

// ProtoRef is a convenience function for computing the ref of a serialized protobuf.
func ProtoRef(m proto.Message) (Ref, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return Zero, err
	}
	return RefOf(b), nil
}
