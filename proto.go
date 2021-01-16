package bs

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
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
func PutProto(ctx context.Context, s Store, m proto.Message) (Ref, bool, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, false, errors.Wrap(err, "marshaling protobuf")
	}

	return s.Put(ctx, b)
}

// Type computes the type protobuf of a given protobuf.
// This is the protobuf's "descriptor,"
// converted to its own protobuf.
// See https://godoc.org/google.golang.org/protobuf/reflect/protoreflect#hdr-Protocol_Buffer_Descriptors.
func Type(m proto.Message) proto.Message {
	return protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())
}

// TypeBlob computes the type blob of a given protobuf.
// This is the marshaled Type().
func TypeBlob(m proto.Message) (Blob, error) {
	t := Type(m)
	b, err := proto.Marshal(t)
	return Blob(b), err
}

// TypeRef computes the type ref of a given protobuf.
// This is the ref of the TypeBlob().
func TypeRef(m proto.Message) (Ref, error) {
	b, err := TypeBlob(m)
	if err != nil {
		return Ref{}, err
	}
	return b.Ref(), nil
}

// ProtoRef is a convenience function for computing the ref of a serialized protobuf.
func ProtoRef(m proto.Message) (Ref, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, err
	}
	return Blob(b).Ref(), nil
}

var (
	// TypeTypeRef is the metatype ref: the ref of the type blob of type blobs.
	TypeTypeRef Ref

	// TypeTypeBlob is the metatype blob: the type blob of type blobs.
	TypeTypeBlob Blob
)

func init() {
	t := Type(&descriptorpb.DescriptorProto{})

	var err error
	TypeTypeBlob, err = TypeBlob(t)
	if err != nil {
		panic(err)
	}

	TypeTypeRef = TypeTypeBlob.Ref()
}
