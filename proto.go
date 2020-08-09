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
	b, _, err := g.Get(ctx, ref)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, m)
}

func PutProto(ctx context.Context, s Store, m proto.Message) (Ref, bool, error) {
	typeProto := Type(m)

	var typeRef Ref
	if _, ok := m.(*descriptorpb.DescriptorProto); ok {
		typeRef = TypeTypeRef
	} else {
		var err error

		typeRef, _, err = PutProto(ctx, s, typeProto)
		if err != nil {
			return Ref{}, false, errors.Wrap(err, "storing protobuf type")
		}
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, false, errors.Wrap(err, "marshaling protobuf")
	}

	return s.Put(ctx, b, &typeRef)
}

func Type(m proto.Message) proto.Message {
	return protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())
}

func TypeBlob(m proto.Message) (Blob, error) {
	t := Type(m)
	b, err := proto.Marshal(t)
	return Blob(b), err
}

func TypeRef(m proto.Message) (Ref, error) {
	b, err := TypeBlob(m)
	if err != nil {
		return Ref{}, err
	}
	return b.Ref(), nil
}

var (
	TypeTypeRef  Ref
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
