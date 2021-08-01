package rpc

import (
	context "context"
	"time"

	"github.com/pkg/errors"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bobg/bs"
	"github.com/bobg/bs/anchor"
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

// ErrNotAnchorStore is the error returned when trying to call an anchor.Store method on a plain bs.Store.
// It is represented with codes.Unimplemented in RPC status objects.
var ErrNotAnchorStore = errors.New("not an anchor store")

func (s *Server) GetAnchor(ctx context.Context, req *GetAnchorRequest) (*GetAnchorResponse, error) {
	astore, ok := s.s.(anchor.Store)
	if !ok {
		return nil, status.Error(codes.Unimplemented, ErrNotAnchorStore.Error())
	}

	ref, err := astore.GetAnchor(ctx, req.Name, req.At.AsTime())
	if err == bs.ErrNotFound {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return nil, err
	}

	return &GetAnchorResponse{Ref: ref[:]}, nil
}

var singletonPutAnchorResponse PutAnchorResponse

func (s *Server) PutAnchor(ctx context.Context, req *PutAnchorRequest) (*PutAnchorResponse, error) {
	astore, ok := s.s.(anchor.Store)
	if !ok {
		return nil, status.Error(codes.Unimplemented, ErrNotAnchorStore.Error())
	}

	err := astore.PutAnchor(ctx, req.Name, bs.RefFromBytes(req.Ref), req.At.AsTime())
	return &singletonPutAnchorResponse, err
}

func (s *Server) ListAnchors(req *ListAnchorsRequest, srv Store_ListAnchorsServer) error {
	astore, ok := s.s.(anchor.Store)
	if !ok {
		return status.Error(codes.Unimplemented, ErrNotAnchorStore.Error())
	}

	return astore.ListAnchors(srv.Context(), req.Start, func(name string, ref bs.Ref, at time.Time) error {
		return srv.Send(&ListAnchorsResponse{Name: name, Ref: ref[:], At: timestamppb.New(at)})
	})
}
