package replica

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/store"
)

var _ bs.Store = (*Store)(nil)

// Store is a blob store that delegates reads and writes to two sets of nested stores.
// One set is synchronous:
// writes to all of these must succeed before a call to Put returns,
// and an error from any will cause Put to fail.
// The other set is asynchronous:
// a call to Put queues writes on these stores but does not wait for them to finish.
// However, if any asynchronous write encounters an error,
// the whole Store is put into an error state and further operations will fail.
type Store struct {
	sync   []bs.Store
	async  []asyncChans
	cancel context.CancelFunc

	mu  sync.Mutex // protects err
	err error      // the error from an async goroutine, if any
}

type asyncChans struct {
	blobs chan<- bs.Blob
	errs  <-chan error
}

// New produces a new Store.
// The set of synchronous stores must be non-empty.
// The set of asynchronous stores may be empty.
// If there are any asynchronous stores,
// goroutines are launched for them,
// and canceling the given context object causes those to exit,
// placing the Store in an error state.
//
// Normally, writes to asynchronous stores do not block calls to Put,
// but the queue for each nested store has a fixed length given by n,
// which must be 1 or greater.
// If any async store falls too far behind,
// Put will block until all requests can be queued.
func New(ctx context.Context, sync []bs.Store, async []bs.Store, n int) *Store {
	result := &Store{sync: sync}

	if len(async) > 0 {
		ctx, result.cancel = context.WithCancel(ctx)

		selectCases := make([]reflect.SelectCase, 1+len(async))

		for i, a := range async {
			var (
				blobs = make(chan bs.Blob, n)
				errs  = make(chan error, 1)
			)

			result.async = append(result.async, asyncChans{blobs: blobs, errs: errs})

			selectCases[i].Dir = reflect.SelectRecv
			selectCases[i].Chan = reflect.ValueOf(errs)

			a := a
			go runAsync(ctx, a, blobs, errs)
		}

		selectCases[len(async)].Dir = reflect.SelectRecv
		selectCases[len(async)].Chan = reflect.ValueOf(ctx.Done())

		go func() {
			_, errval, ok := reflect.Select(selectCases)
			if ok {
				result.cancel()
				result.mu.Lock()
				result.err = errval.Interface().(error)
				result.mu.Unlock()
			}
		}()
	}

	return result
}

// Runs as a goroutine until ctx is canceled or an error occurs (which it writes to errs).
func runAsync(ctx context.Context, store bs.Store, blobs <-chan bs.Blob, errs chan<- error) {
	defer close(errs)

	for {
		select {
		case <-ctx.Done():
			errs <- ctx.Err()
			return

		case blob := <-blobs:
			_, _, err := store.Put(ctx, blob)
			if err != nil {
				errs <- err
				return
			}
		}
	}
}

// Put implements bs.Store.Put.
// The blob is stored in all synchronous nested stores.
// An error from any of them causes Put to return an error.
//
// Some nested stores may already have the blob and others may not,
// in which case the value of `added`
// (the boolean return value)
// is indeterminate.
// (It is determined by the first nested store to finish.)
//
// A request to write the blob is queued for any asynchronous nested stores.
// Normally this does not block the call to Put,
// but if any async store falls too far behind,
// Put must wait for space to open in its request queue before proceeding.
// The size of this queue is given by the int passed to New.
func (s *Store) Put(ctx context.Context, blob bs.Blob) (bs.Ref, bool, error) {
	s.mu.Lock()
	err := s.err
	s.mu.Unlock()
	if err != nil {
		return bs.Zero, false, errors.Wrap(err, "in async-store goroutine")
	}

	type pairType struct {
		ref   bs.Ref
		added bool
	}

	g, ctx := errgroup.WithContext(ctx)
	ch := make(chan pairType, len(s.sync))
	for _, store := range s.sync {
		store := store
		g.Go(func() error {
			ref, added, err := store.Put(ctx, blob)
			if err != nil {
				return err
			}
			ch <- pairType{ref: ref, added: added}
			return nil
		})
	}

	for _, a := range s.async {
		select {
		case <-ctx.Done():
			return bs.Zero, false, ctx.Err()

		case a.blobs <- blob:
		}
	}

	err = g.Wait()
	if err != nil {
		if s.cancel != nil {
			s.cancel()
		}
		return bs.Zero, false, err
	}
	pair := <-ch
	return pair.ref, pair.added, nil
}

// Get implements bs.Getter.
// It delegates the request to all of the synchronous stores in s.
// returning the result from the first one to respond without error
// and canceling the request to the others.
// If all synchronous stores respond with an error,
// one of those errors is returned.
func (s *Store) Get(ctx context.Context, ref bs.Ref) ([]byte, error) {
	if err := s.checkErr(); err != nil {
		return nil, errors.Wrap(err, "in async-store goroutine")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var g errgroup.Group

	ch := make(chan []byte)
	for _, store := range s.sync {
		store := store
		g.Go(func() error {
			blob, err := store.Get(ctx, ref)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ch <- blob:
			}
			return nil
		})
	}

	var (
		blob []byte
		ok   bool
		err  error
		done = make(chan struct{})
	)

	go func() {
		blob, ok = <-ch
		done <- struct{}{}
	}()

	go func() {
		err = g.Wait()
		done <- struct{}{}
	}()

	<-done
	if ok {
		return blob, nil
	}
	return nil, err
}

// ListRefs implements bs.Getter.
// It delegates the request to all of the synchronous stores in s
// and synthesizes the result from the union of their refs.
func (s *Store) ListRefs(ctx context.Context, start bs.Ref, f func(bs.Ref) error) error {
	if err := s.checkErr(); err != nil {
		return errors.Wrap(err, "in async-store goroutine")
	}

	chans := make([]chan bs.Ref, len(s.sync))
	for i := 0; i < len(s.sync); i++ {
		chans[i] = make(chan bs.Ref, 1)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	for i, store := range s.sync {
		var (
			i     = i
			store = store
		)
		g.Go(func() error {
			defer close(chans[i])
			return store.ListRefs(ctx, start, func(ref bs.Ref) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case chans[i] <- ref:
					return nil
				}
			})
		})
	}

	last := start
	next := make([]bs.Ref, len(s.sync))
	for i, ch := range chans {
		next[i] = <-ch
	}

	for {
		var (
			best      bs.Ref
			bestIndex int
		)
		for i, ref := range next {
			if ref.IsZero() {
				continue
			}
			if ref == last {
				ref = <-chans[i]
				next[i] = ref
			}
			if best.IsZero() {
				best, bestIndex = ref, i
				continue
			}
			if ref.Less(best) {
				best, bestIndex = ref, i
			}
		}
		if best.IsZero() {
			break
		}
		err := f(best)
		if err != nil {
			return err
		}
		last = best
		next[bestIndex] = <-chans[bestIndex]
	}

	return g.Wait()
}

func (s *Store) checkErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func init() {
	store.Register("replica", func(ctx context.Context, conf map[string]interface{}) (bs.Store, error) {
		var (
			syncStores  []bs.Store
			asyncStores []bs.Store
			queueLen    int64
		)

		sync, ok := conf["sync"].([]map[string]interface{})
		if !ok {
			return nil, errors.New(`missing "sync" parameter`)
		}
		for _, nested := range sync {
			nestedType, ok := nested["type"].(string)
			if !ok {
				return nil, errors.New(`"sync" item missing "type"`)
			}
			nestedStore, err := store.Create(ctx, nestedType, nested)
			if err != nil {
				return nil, errors.Wrap(err, "creating nested sync store")
			}
			syncStores = append(syncStores, nestedStore)
		}

		async, ok := conf["async"].([]map[string]interface{})
		if ok {
			for _, nested := range async {
				nestedType, ok := nested["type"].(string)
				if !ok {
					return nil, errors.New(`"async" item missing "type"`)
				}
				nestedStore, err := store.Create(ctx, nestedType, nested)
				if err != nil {
					return nil, errors.Wrap(err, "creating nested async store")
				}
				asyncStores = append(asyncStores, nestedStore)
			}
		}

		if queueLenNum, ok := conf["queuelen"].(json.Number); ok {
			var err error
			queueLen, err = queueLenNum.Int64()
			if err != nil {
				return nil, errors.Wrapf(err, "parsing queue length %v", queueLenNum)
			}
		} else {
			queueLen = 10
		}

		return New(ctx, syncStores, asyncStores, int(queueLen)), nil
	})
}
