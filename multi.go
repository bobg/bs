package bs

import "context"

// GetMulti is a convenience function for use by Store implementations
// that need no special batching logic for their GetMulti methods.
// This implementation simply calls s.Get for each ref in refs.
func GetMulti(ctx context.Context, g Getter, refs []Ref) (GetMultiResult, error) {
	result := make(GetMultiResult)
	for _, ref := range refs {
		ref := ref
		result[ref] = func(ctx context.Context) (Blob, error) {
			return g.Get(ctx, ref)
		}
	}
	return result, nil
}

// PutMulti is a convenience function for use by Store implementations
// that need no special batching logic for their PutMulti methods.
// This implementation simply calls s.Put for each blob in blobs.
func PutMulti(ctx context.Context, s Store, blobs []Blob) (PutMultiResult, error) {
	result := make(PutMultiResult, 0, len(blobs))
	for _, b := range blobs {
		ref, added, err := s.Put(ctx, b)
		result = append(result, func(_ context.Context) (Ref, bool, error) {
			return ref, added, err
		})
	}
	return result, nil
}
