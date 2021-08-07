package rpc

import (
	context "context"

	"github.com/pkg/errors"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"

	"github.com/bobg/bs"
)

var _ StoreServer = &Server{}

type Server struct {
	UnimplementedStoreServer // "All implementations must embed UnimplementedStoreServer for forward compatibility."

	s bs.Store
}

func NewServer(s bs.Store) *Server {
	return &Server{s: s}
}

func (s *Server) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	blob, err := s.s.Get(ctx, bs.RefFromBytes(req.Ref))
	if errors.Is(err, bs.ErrNotFound) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &GetResponse{Blob: blob}, err
}

func (s *Server) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	ref, added, err := s.s.Put(ctx, req.Blob)
	if err != nil {
		return nil, err
	}
	return &PutResponse{Ref: ref[:], Added: added}, nil
}

func (s *Server) ListRefs(req *ListRefsRequest, srv Store_ListRefsServer) error {
	return s.s.ListRefs(srv.Context(), bs.RefFromBytes(req.Start), func(ref bs.Ref) error {
		return srv.Send(&ListRefsResponse{Ref: ref[:]})
	})
}
