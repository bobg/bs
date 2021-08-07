// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.6.1
// source: anchor.proto

package anchor

import (
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Anchor is a timestamped ref.
type Anchor struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Ref []byte               `protobuf:"bytes,1,opt,name=ref,proto3" json:"ref,omitempty"`
	At  *timestamp.Timestamp `protobuf:"bytes,2,opt,name=at,proto3" json:"at,omitempty"`
}

func (x *Anchor) Reset() {
	*x = Anchor{}
	if protoimpl.UnsafeEnabled {
		mi := &file_anchor_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Anchor) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Anchor) ProtoMessage() {}

func (x *Anchor) ProtoReflect() protoreflect.Message {
	mi := &file_anchor_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Anchor.ProtoReflect.Descriptor instead.
func (*Anchor) Descriptor() ([]byte, []int) {
	return file_anchor_proto_rawDescGZIP(), []int{0}
}

func (x *Anchor) GetRef() []byte {
	if x != nil {
		return x.Ref
	}
	return nil
}

func (x *Anchor) GetAt() *timestamp.Timestamp {
	if x != nil {
		return x.At
	}
	return nil
}

var File_anchor_proto protoreflect.FileDescriptor

var file_anchor_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x61, 0x6e, 0x63, 0x68, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x06,
	0x61, 0x6e, 0x63, 0x68, 0x6f, 0x72, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x46, 0x0a, 0x06, 0x41, 0x6e, 0x63, 0x68, 0x6f,
	0x72, 0x12, 0x10, 0x0a, 0x03, 0x72, 0x65, 0x66, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x03,
	0x72, 0x65, 0x66, 0x12, 0x2a, 0x0a, 0x02, 0x61, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x02, 0x61, 0x74, 0x42,
	0x0a, 0x5a, 0x08, 0x2e, 0x3b, 0x61, 0x6e, 0x63, 0x68, 0x6f, 0x72, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_anchor_proto_rawDescOnce sync.Once
	file_anchor_proto_rawDescData = file_anchor_proto_rawDesc
)

func file_anchor_proto_rawDescGZIP() []byte {
	file_anchor_proto_rawDescOnce.Do(func() {
		file_anchor_proto_rawDescData = protoimpl.X.CompressGZIP(file_anchor_proto_rawDescData)
	})
	return file_anchor_proto_rawDescData
}

var file_anchor_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_anchor_proto_goTypes = []interface{}{
	(*Anchor)(nil),              // 0: anchor.Anchor
	(*timestamp.Timestamp)(nil), // 1: google.protobuf.Timestamp
}
var file_anchor_proto_depIdxs = []int32{
	1, // 0: anchor.Anchor.at:type_name -> google.protobuf.Timestamp
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_anchor_proto_init() }
func file_anchor_proto_init() {
	if File_anchor_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_anchor_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Anchor); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_anchor_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_anchor_proto_goTypes,
		DependencyIndexes: file_anchor_proto_depIdxs,
		MessageInfos:      file_anchor_proto_msgTypes,
	}.Build()
	File_anchor_proto = out.File
	file_anchor_proto_rawDesc = nil
	file_anchor_proto_goTypes = nil
	file_anchor_proto_depIdxs = nil
}
