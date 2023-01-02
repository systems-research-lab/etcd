// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	store         *kvstore
	confChangeC   chan<- raftpb.ConfChange
	confChangeCV2 chan<- raftpb.ConfChangeV2
}

type data struct {
	id  uint64
	ip  string
	opr uint64
}

type ConfChangeJson struct {
	data []data `json:"data"`
	q    uint64 `json:"q"`
}

type newjson struct {
	q uint64 `json:"q"`
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.RequestURI
	defer r.Body.Close()
	switch {
	case r.Method == "PUT":
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		h.store.Propose(key, string(v))

		// Optimistic-- no waiting for ack from raft. Value is not yet
		// committed so a subsequent GET on the key may return old value
		w.WriteHeader(http.StatusNoContent)
	case r.Method == "GET":
		if v, ok := h.store.Lookup(key); ok {
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case r.Method == "POST":
		url, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}
		//added by shireen
		if key == "/confchange" {
			byt := []byte(url)

			//var dat map[string]interface{}
			var dat = map[string][]string{}

			if err := json.Unmarshal(byt, &dat); err != nil {
				panic(err)
			}
			ccv2 := raftpb.ConfChangeV2{
				Transition: 3,
				Changes:    nil,
				Context:    nil,
				ConfIndex:  0,
				ConfTerm:   0,
			}
			var ips []string
			var ids []string
			var operations []string

			for key, value := range dat {
				if key == "q" {
					var q uint64
					q, err = strconv.ParseUint(value[0], 0, 64)
					if err != nil {
						log.Printf("Failed to convert quorum for conf change (%v)\n", err)
						http.Error(w, "Failed on POST", http.StatusBadRequest)
						return
					}
					ccv2.Quorum = q
				} else if key == "ip" {
					ips = value
				} else if key == "id" {
					ids = value
				} else if key == "opr" {
					operations = value
				}
			}
			var ccsA []raftpb.ConfChangeSingle
			if len(operations) == len(ips) && len(ips) == len(ids) {
				j := 0
				for range ids {
					var ccs raftpb.ConfChangeSingle
					var opr uint64
					opr, err = strconv.ParseUint(operations[j], 0, 64)
					if err != nil {
						log.Printf("Failed to convert operation for conf change (%v)\n", err)
						http.Error(w, "Failed on POST", http.StatusBadRequest)
						return
					}
					if opr == 0 {
						ccs.Type = raftpb.ConfChangeAddNode
					} else if opr == 1 {
						ccs.Type = raftpb.ConfChangeRemoveNode
					}
					var id uint64
					id, err = strconv.ParseUint(ids[j], 0, 64)
					if err != nil {
						log.Printf("Failed to convert ID for conf change (%v)\n", err)
						http.Error(w, "Failed on POST", http.StatusBadRequest)
						return
					}
					ccs.NodeID = id
					ccs.Context = []byte(ips[j])
					ccsA = append(ccsA, ccs)
					j++
				}
				ccv2.Changes = ccsA
			} else {
				panic("length not equal for ips and ids")
			}

			h.confChangeCV2 <- ccv2
		} else {
			nodeId, err := strconv.ParseUint(key[1:], 0, 64)
			if err != nil {
				log.Printf("Failed to convert ID for conf change (%v)\n", err)
				http.Error(w, "Failed on POST", http.StatusBadRequest)
				return
			}

			cc := raftpb.ConfChange{
				Type:    raftpb.ConfChangeAddNode,
				NodeID:  nodeId,
				Context: url,
			}
			h.confChangeC <- cc

			// As above, optimistic that raft will apply the conf change
			w.WriteHeader(http.StatusNoContent)
		}
	case r.Method == "DELETE":
		nodeId, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: nodeId,
		}
		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "PUT")
		w.Header().Add("Allow", "GET")
		w.Header().Add("Allow", "POST")
		w.Header().Add("Allow", "DELETE")
		w.Header().Add("Access-Control-Allow-Headers", "content-type")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func serveHttpKVAPI(kv *kvstore, port int, confChangeC chan<- raftpb.ConfChange, confChangeCV2 chan<- raftpb.ConfChangeV2, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:         kv,
			confChangeC:   confChangeC,
			confChangeCV2: confChangeCV2,
		},
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// exit when raft goes down
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
