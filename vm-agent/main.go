package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/linuxkit/virtsock/pkg/vsock"
	"github.com/lxc/lxd/shared/api"
	"github.com/pkg/errors"
)

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/state", stateHandler)
	r.HandleFunc("/1.0/exec", func(w http.ResponseWriter, r *http.Request) {
		err := execHandler(w, r).Render(w)
		if err != nil {
			log.Println(errors.Wrap(err, "Failed to handle exec request"))
		}
	})
	r.HandleFunc("/1.0/operations/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		id := mux.Vars(r)["id"]
		var body *api.Operation

		// First check if the query is for a local operation from this node
		op, err := operationGetInternal(id)
		if err == nil {
			_, body, err = op.Render()
			if err != nil {
				log.Println(errors.Wrap(err, "Failed to handle operations request"))
			}

			resp := SyncResponse(true, body)
			resp.Render(w)
		}

		log.Println(errors.Wrap(err, "Failed to handle operations request"))
	})
	r.HandleFunc("/1.0/operations/{id}/websocket", func(w http.ResponseWriter, r *http.Request) {
		id := mux.Vars(r)["id"]

		// First check if the query is for a local operation from this node
		op, err := operationGetInternal(id)
		if err == nil {
			resp := &OperationWebSocket{r, op}
			err = resp.Render(w)
			if err != nil {
				log.Println(errors.Wrap(err, "Failed to handle operations request for websocket"))
			}
		}
	})

	http.Handle("/", r)

	l, err := vsock.Listen(vsock.CIDAny, 8443)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Fatal(http.Serve(l, nil))
}

func stateHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(renderState())
}
