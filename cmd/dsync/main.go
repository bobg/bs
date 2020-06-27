package main

import (
	"context"
	stderrs "errors"
	"flag"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"

	"github.com/rjeczalik/notify"

	"github.com/bobg/bs"
	"github.com/bobg/bs/dsync"
	"github.com/bobg/bs/file"
)

func main() {
	var (
		primary = flag.Bool("primary", false, "run as primary")
		replica = flag.Bool("replica", false, "run as replica")
		root    = flag.String("root", "", "root dir")
		addr    = flag.String("addr", "localhost:", "primary: replica is at this addr; replica: listen on this addr")
	)
	flag.Parse()

	if !*primary && !*replica {
		log.Fatal("must supply one of -primary or -replica")
	}
	if *primary && *replica {
		log.Fatal("cannot supply both -primary and -replica")
	}
	if *root == "" {
		log.Fatal("must specify root")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		sig := <-sigCh
		log.Printf("got signal %s", sig)
		cancel()
	}()

	tmpdir, err := ioutil.TempDir("", "dsync")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	fstore := file.New(tmpdir)

	log.Printf("Created file store at %s", tmpdir)

	var wg sync.WaitGroup

	if *primary {
		doPrimary(ctx, &wg, fstore, *root, "http://"+*addr)
	} else {
		doReplica(ctx, &wg, fstore, *root, *addr)
	}

	wg.Wait()
}

func doPrimary(ctx context.Context, wg *sync.WaitGroup, s bs.Store, root, replicaURL string) {
	anchorURL, err := url.Parse(replicaURL + "/anchor")
	if err != nil {
		log.Fatal(err)
	}

	var (
		refs    = make(chan bs.Ref)
		anchors = make(chan dsync.AnchorTuple)
	)

	streamer := dsync.NewStreamer(s, refs, anchors)
	t := &dsync.Tree{S: streamer, Root: root}

	// This goroutine watches for new refs and anchors,
	// sending them to the replica.
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			// There are two selects here
			// (the first one non-blocking),
			// to ensure that all pending blobs are handled
			// before any pending anchors.

			select {
			case <-ctx.Done():
				log.Print("context canceled, exiting new-blob watcher")
				return

			case ref := <-refs:
				err := getSendBlob(ctx, t, ref, replicaURL)
				if err != nil {
					log.Printf("ERROR sending blob %s: %s", ref, err)
				}

				// Prevent falling through to the next select
				// if there's another blob to process.
				continue

			default:
			}

			select {
			case <-ctx.Done():
				log.Print("context canceled, exiting new-blob watcher")
				return

			case ref := <-refs:
				err := getSendBlob(ctx, t, ref, replicaURL)
				if err != nil {
					log.Printf("ERROR sending blob %s: %s", ref, err)
				}

			case tuple := <-anchors:
				v := url.Values{}
				v.Set("ref", tuple.Ref.String())
				v.Set("a", string(tuple.A))

				anchorURL.RawQuery = v.Encode()

				req, err := http.NewRequest("POST", anchorURL.String(), nil)
				if err != nil {
					log.Printf("ERROR constructing POST request for %s: %s", anchorURL, err)
					continue
				}

				var client http.Client
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("ERROR sending anchor %s (ref %s): %s", tuple.A, tuple.Ref, err)
					continue
				}
				if resp.Body != nil {
					resp.Body.Close()
				}
			}
		}
	}()

	// This must come after the launch of the goroutine above
	// because it wants to write to the `refs` and `anchors` channels
	// and will block if there's nothing to consume them.
	err = t.Ingest(ctx, root)
	if err != nil {
		log.Fatal(err)
	}

	fsch := make(chan notify.EventInfo, 100)

	// This goroutine monitors the filesystem watcher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer notify.Stop(fsch)

		for {
			select {
			case <-ctx.Done():
				log.Print("context canceled, exiting filesystem watcher")
				return

			case ev := <-fsch:
				err := t.FileChanged(ctx, ev.Path())
				if err != nil {
					log.Printf("ERROR handling change of file %s: %s", ev.Path(), err)
				}
			}
		}
	}()

	err = notify.Watch(root+"/...", fsch, notify.All)
	if err != nil {
		log.Fatal(err)
	}
}

func doReplica(ctx context.Context, wg *sync.WaitGroup, s bs.Store, root, addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	t := &dsync.Tree{S: s, Root: root}
	err = t.Ingest(ctx, root)
	if err != nil {
		log.Fatal(err)
	}

	rep := (*replica)(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/blob", rep.handleBlob)
	mux.HandleFunc("/anchor", rep.handleAnchor)

	srv := &http.Server{Handler: mux}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		srv.Shutdown(context.TODO())
	}()

	log.Printf("listening on %s", l.Addr())
	err = srv.Serve(l)
	if !stderrs.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
