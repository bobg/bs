package rpc

import (
	context "context"

	"github.com/pkg/errors"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
)

var _ StoreServer = &Server{}

// Server is the type of an RPC server that is an interface to a bs.Store.
// Use Client to communicate with a Server (qv).
type Server struct {
	UnimplementedStoreServer // "All implementations must embed UnimplementedStoreServer for forward compatibility."

	s bs.Store
}

// NewServer produces a new Server interacting with a given bs.Store.
// The given store may optionally be an anchor.Store as well.
func NewServer(s bs.Store) *Server {
	return &Server{s: s}
}

// Get implements StoreServer.Get.
func (s *Server) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	blob, err := s.s.Get(ctx, bs.RefFromBytes(req.Ref))
	if errors.Is(err, bs.ErrNotFound) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &GetResponse{Blob: blob}, err
}

// Put implements StoreServer.Put.
func (s *Server) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	ref, added, err := s.s.Put(ctx, req.Blob)
	if err != nil {
		return nil, err
	}
	return &PutResponse{Ref: ref[:], Added: added}, nil
}

// PutType implements StoreServer.PutType.
func (s *Server) PutType(ctx context.Context, req *PutTypeRequest) (*PutTypeResponse, error) {
	ts, ok := s.s.(bs.TStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, bs.ErrNotTStore.Error())
	}
	ref := bs.RefFromBytes(req.Ref)
	err := ts.PutType(ctx, ref, req.Type)
	return &PutTypeResponse{}, err
}

// ListRefs implements StoreServer.ListRefs.
func (s *Server) ListRefs(req *ListRefsRequest, srv Store_ListRefsServer) error {
	return s.s.ListRefs(srv.Context(), bs.RefFromBytes(req.Start), func(ref bs.Ref) error {
		return srv.Send(&ListRefsResponse{Ref: ref[:]})
	})
}

// AnchorMapRef implements StoreServer.AnchorMapRef.
func (s *Server) AnchorMapRef(ctx context.Context, req *AnchorMapRefRequest) (*AnchorMapRefResponse, error) {
	astore, ok := s.s.(anchor.Getter)
	if !ok {
		return nil, status.Error(codes.Unimplemented, anchor.ErrNotAnchorStore.Error())
	}
	ref, err := astore.AnchorMapRef(ctx)
	if errors.Is(err, anchor.ErrNoAnchorMap) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return nil, err
	}
	return &AnchorMapRefResponse{Ref: ref[:]}, nil
}

// UpdateAnchorMap implements StoreServer.UpdateAnchorMap.
// TODO: revisit this implementation, something seems fishy about it.
func (s *Server) UpdateAnchorMap(ctx context.Context, req *UpdateAnchorMapRequest) (*UpdateAnchorMapResponse, error) {
	astore, ok := s.s.(anchor.Store)
	if !ok {
		return nil, status.Error(codes.Unimplemented, anchor.ErrNotAnchorStore.Error())
	}
	err := astore.UpdateAnchorMap(ctx, func(bs.Ref) (bs.Ref, error) {
		return bs.RefFromBytes(req.NewRef), nil
	})
	if errors.Is(err, anchor.ErrUpdateConflict) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &UpdateAnchorMapResponse{}, err
}
