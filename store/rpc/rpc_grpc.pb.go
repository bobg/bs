// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package rpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// StoreClient is the client API for Store service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type StoreClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error)
	ListRefs(ctx context.Context, in *ListRefsRequest, opts ...grpc.CallOption) (Store_ListRefsClient, error)
	AnchorMapRef(ctx context.Context, in *AnchorMapRefRequest, opts ...grpc.CallOption) (*AnchorMapRefResponse, error)
	UpdateAnchorMap(ctx context.Context, in *UpdateAnchorMapRequest, opts ...grpc.CallOption) (*UpdateAnchorMapResponse, error)
}

type storeClient struct {
	cc grpc.ClientConnInterface
}

func NewStoreClient(cc grpc.ClientConnInterface) StoreClient {
	return &storeClient{cc}
}

func (c *storeClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/rpc.Store/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *storeClient) Put(ctx context.Context, in *PutRequest, opts ...grpc.CallOption) (*PutResponse, error) {
	out := new(PutResponse)
	err := c.cc.Invoke(ctx, "/rpc.Store/Put", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *storeClient) ListRefs(ctx context.Context, in *ListRefsRequest, opts ...grpc.CallOption) (Store_ListRefsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_Store_serviceDesc.Streams[0], "/rpc.Store/ListRefs", opts...)
	if err != nil {
		return nil, err
	}
	x := &storeListRefsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Store_ListRefsClient interface {
	Recv() (*ListRefsResponse, error)
	grpc.ClientStream
}

type storeListRefsClient struct {
	grpc.ClientStream
}

func (x *storeListRefsClient) Recv() (*ListRefsResponse, error) {
	m := new(ListRefsResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *storeClient) AnchorMapRef(ctx context.Context, in *AnchorMapRefRequest, opts ...grpc.CallOption) (*AnchorMapRefResponse, error) {
	out := new(AnchorMapRefResponse)
	err := c.cc.Invoke(ctx, "/rpc.Store/AnchorMapRef", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *storeClient) UpdateAnchorMap(ctx context.Context, in *UpdateAnchorMapRequest, opts ...grpc.CallOption) (*UpdateAnchorMapResponse, error) {
	out := new(UpdateAnchorMapResponse)
	err := c.cc.Invoke(ctx, "/rpc.Store/UpdateAnchorMap", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// StoreServer is the server API for Store service.
// All implementations must embed UnimplementedStoreServer
// for forward compatibility
type StoreServer interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Put(context.Context, *PutRequest) (*PutResponse, error)
	ListRefs(*ListRefsRequest, Store_ListRefsServer) error
	AnchorMapRef(context.Context, *AnchorMapRefRequest) (*AnchorMapRefResponse, error)
	UpdateAnchorMap(context.Context, *UpdateAnchorMapRequest) (*UpdateAnchorMapResponse, error)
	mustEmbedUnimplementedStoreServer()
}

// UnimplementedStoreServer must be embedded to have forward compatible implementations.
type UnimplementedStoreServer struct {
}

func (*UnimplementedStoreServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (*UnimplementedStoreServer) Put(context.Context, *PutRequest) (*PutResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Put not implemented")
}
func (*UnimplementedStoreServer) ListRefs(*ListRefsRequest, Store_ListRefsServer) error {
	return status.Errorf(codes.Unimplemented, "method ListRefs not implemented")
}
func (*UnimplementedStoreServer) AnchorMapRef(context.Context, *AnchorMapRefRequest) (*AnchorMapRefResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AnchorMapRef not implemented")
}
func (*UnimplementedStoreServer) UpdateAnchorMap(context.Context, *UpdateAnchorMapRequest) (*UpdateAnchorMapResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateAnchorMap not implemented")
}
func (*UnimplementedStoreServer) mustEmbedUnimplementedStoreServer() {}

func RegisterStoreServer(s *grpc.Server, srv StoreServer) {
	s.RegisterService(&_Store_serviceDesc, srv)
}

func _Store_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StoreServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.Store/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StoreServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Store_Put_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(PutRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StoreServer).Put(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.Store/Put",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StoreServer).Put(ctx, req.(*PutRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Store_ListRefs_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(ListRefsRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(StoreServer).ListRefs(m, &storeListRefsServer{stream})
}

type Store_ListRefsServer interface {
	Send(*ListRefsResponse) error
	grpc.ServerStream
}

type storeListRefsServer struct {
	grpc.ServerStream
}

func (x *storeListRefsServer) Send(m *ListRefsResponse) error {
	return x.ServerStream.SendMsg(m)
}

func _Store_AnchorMapRef_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AnchorMapRefRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StoreServer).AnchorMapRef(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.Store/AnchorMapRef",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StoreServer).AnchorMapRef(ctx, req.(*AnchorMapRefRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Store_UpdateAnchorMap_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UpdateAnchorMapRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(StoreServer).UpdateAnchorMap(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/rpc.Store/UpdateAnchorMap",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(StoreServer).UpdateAnchorMap(ctx, req.(*UpdateAnchorMapRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _Store_serviceDesc = grpc.ServiceDesc{
	ServiceName: "rpc.Store",
	HandlerType: (*StoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _Store_Get_Handler,
		},
		{
			MethodName: "Put",
			Handler:    _Store_Put_Handler,
		},
		{
			MethodName: "AnchorMapRef",
			Handler:    _Store_AnchorMapRef_Handler,
		},
		{
			MethodName: "UpdateAnchorMap",
			Handler:    _Store_UpdateAnchorMap_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ListRefs",
			Handler:       _Store_ListRefs_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "rpc.proto",
}
