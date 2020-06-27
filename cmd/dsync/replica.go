package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/bobg/bs"
	"github.com/bobg/bs/dsync"
)

type replica dsync.Tree

func (rep *replica) handleBlob(w http.ResponseWriter, req *http.Request) {
	blob, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	ref, added, err := rep.S.Put(req.Context(), blob)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if added {
		log.Printf("received blob %s", ref)
	} else {
		log.Printf("received duplicate blob %s", ref)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (rep *replica) handleAnchor(w http.ResponseWriter, req *http.Request) {
	vals := req.URL.Query()
	a := vals.Get("a")
	refHex := vals.Get("ref")
	ref, err := bs.RefFromHex(refHex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = (*dsync.Tree)(rep).ReplicaAnchor(req.Context(), bs.Anchor(a), ref)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("received anchor %s: %s", a, ref)

	w.WriteHeader(http.StatusNoContent)
}
