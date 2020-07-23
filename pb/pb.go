package pb

import (
	"bytes"
	"context"
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/bobg/bs"
)

// Type produces the type of the protobuf message (its descriptor) as a Blob.
func Type(m proto.Message) (bs.Blob, error) {
	return proto.Marshal(protodesc.ToDescriptorProto(m.ProtoReflect().Descriptor()))
}

// Put stores a protobuf as a blob together with its type information.
//
// The type is turned into a bs.Blob with Type and stored separately.
// The type blob's ref is stored in a pb.Blob together with the serialized protobuf
// and stored.
func Put(ctx context.Context, s bs.Store, m proto.Message) (bs.Ref, bool, error) {
	t, err := Type(m)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "getting type blob from proto")
	}
	tref, _, err := s.Put(ctx, t)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "storing type blob for proto")
	}
	mb, err := proto.Marshal(m)
	if err != nil {
		return bs.Ref{}, false, errors.Wrap(err, "marshaling proto")
	}

	tblob := &Blob{
		Type: tref[:],
		Blob: mb,
	}
	return bs.PutProto(ctx, s, tblob)
}

// ErrWrongType is the error produced when trying to load a typed blob into the wrong type of protobuf.
var ErrWrongType = errors.New("wrong type")

// Load gets the pb.Blob (typed blob) at ref,
// checks that its type descriptor matches the type of m,
// and unmarshals its nested Blob into m.
func Load(ctx context.Context, g bs.Getter, ref bs.Ref, m proto.Message) error {
	var tblob Blob
	err := bs.GetProto(ctx, g, ref, &tblob)
	if err != nil {
		return errors.Wrap(err, "getting typed blob")
	}
	mType, err := Type(m)
	if err != nil {
		return errors.Wrap(err, "getting protobuf type")
	}
	mTypeRef := mType.Ref()
	if !bytes.Equal(mTypeRef[:], tblob.Type) {
		return ErrWrongType
	}
	err = proto.Unmarshal(tblob.Blob, m)
	return errors.Wrapf(err, "unmarshaling typed blob payload")
}

// Get retrieves the pb.Blob (typed blob) at ref,
// constructs a new *dynamicpb.Message from the described type,
// and loads the nested Blob into it.
// This serializes the same as the original protobuf
// but is not convertible (or type-assertable) to the original protobuf's Go type.
func Get(ctx context.Context, g bs.Getter, ref bs.Ref) (*dynamicpb.Message, error) {
	var tblob Blob
	err := bs.GetProto(ctx, g, ref, &tblob)
	if err != nil {
		return nil, errors.Wrap(err, "getting typed blob")
	}

	tref := bs.RefFromBytes(tblob.Type)
	var dp descriptorpb.DescriptorProto
	err = bs.GetProto(ctx, g, tref, &dp)
	if err != nil {
		return nil, errors.Wrapf(err, "getting descriptor proto at %s", tref)
	}

	md, err := descriptorProtoToMessageDescriptor(&dp)
	if err != nil {
		return nil, errors.Wrapf(err, "manifesting descriptor proto at %s", tref)
	}

	dm := dynamicpb.NewMessage(md)
	err = proto.Unmarshal(tblob.Blob, dm)
	return dm, errors.Wrapf(err, "unmarshaling into protobuf manifested from descriptor proto at %s", tref)
}

func descriptorProtoToMessageDescriptor(dp *descriptorpb.DescriptorProto) (protoreflect.MessageDescriptor, error) {
	name := "x"
	f, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{Name: &name, MessageType: []*descriptorpb.DescriptorProto{dp}}}})
	if err != nil {
		return nil, errors.Wrap(err, "creating Files object")
	}
	if n := f.NumFiles(); n != 1 {
		return nil, fmt.Errorf("created Files object has %d files (want 1)", n)
	}

	var md protoreflect.MessageDescriptor
	f.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		mds := fd.Messages()
		if n := mds.Len(); n != 1 {
			err = fmt.Errorf("got %d messages in created Files object (want 1)", n)
			return false
		}
		md = mds.Get(0)
		return true
	})

	return md, err
}
