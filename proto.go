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
	typeProto := ProtoType(m)
	typeBytes, err := proto.Marshal(typeProto)
	if err != nil {
		return Ref{}, false, errors.Wrap(err, "marshaling protobuf type")
	}
	typeRef, _, err := s.Put(ctx, typeBytes, &typeTypeRef)
	if err != nil {
		return Ref{}, false, errors.Wrap(err, "storing protobuf type")
	}

	b, err := proto.Marshal(m)
	if err != nil {
		return Ref{}, false, errors.Wrap(err, "marshaling protobuf")
	}

	return s.Put(ctx, b, &typeRef)
}

func ProtoType(m proto.Message) proto.Message {
	return protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor())
}

var (
	typeTypeRef  Ref
	typeTypeBlob Blob
)

func init() {
	var (
		dp descriptorpb.DescriptorProto
		p  = ProtoType(&dp)
	)

	typeTypeBytes, err := proto.Marshal(p)
	if err != nil {
		panic(err)
	}
	typeTypeBlob = Blob(typeTypeBytes)
	typeTypeRef = typeTypeBlob.Ref()
}
