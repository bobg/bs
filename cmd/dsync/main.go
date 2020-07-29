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

	"github.com/pkg/errors"

	"github.com/bobg/bs"
	"github.com/bobg/bs/dsync"
	"github.com/bobg/bs/store/file"
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
	t := &dsync.Tree{S: fstore, Root: *root}

	log.Printf("Created file store at %s", tmpdir)

	if *primary {
		doPrimary(ctx, t, "http://"+*addr)
	} else {
		doReplica(ctx, t, *addr)
	}
}

func doPrimary(ctx context.Context, t *dsync.Tree, replicaURL string) {
	anchorURL, err := url.Parse(replicaURL + "/anchor")
	if err != nil {
		log.Fatal(err)
	}

	newRef := func(ref bs.Ref) error {
		return getSendBlob(ctx, t, ref, replicaURL)
	}
	newAnchor := func(tuple dsync.AnchorTuple) error {
		v := url.Values{}
		v.Set("ref", tuple.Ref.String())
		v.Set("a", string(tuple.A))

		anchorURL.RawQuery = v.Encode()

		req, err := http.NewRequest("POST", anchorURL.String(), nil)
		if err != nil {
			return errors.Wrapf(err, "constructing POST request for %s", anchorURL)
		}

		var client http.Client
		resp, err := client.Do(req)
		if err != nil {
			return errors.Wrapf(err, "sending anchor %s (ref %s)", tuple.A, tuple.Ref)
		}
		if resp.Body != nil {
			resp.Body.Close()
		}

		return nil
	}

	err = t.RunPrimary(ctx, newRef, newAnchor)
	if err != nil {
		log.Fatal(err)
	}
}

func doReplica(ctx context.Context, t *dsync.Tree, addr string) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	rootRef, err := t.Ingest(ctx, t.Root)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("ingested root %s at %s", t.Root, rootRef)

	rep := (*replica)(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/blob", rep.handleBlob)
	mux.HandleFunc("/anchor", rep.handleAnchor)

	srv := &http.Server{Handler: mux}

	done := make(chan struct{})

	go func() {
		defer close(done)
		<-ctx.Done()
		srv.Shutdown(context.TODO())
	}()

	log.Printf("listening on %s", l.Addr())
	err = srv.Serve(l)
	if !stderrs.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}

	<-done
}
