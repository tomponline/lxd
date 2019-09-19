package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/lxc/lxd/lxd/util"
	"github.com/lxc/lxd/shared"
	"github.com/lxc/lxd/shared/api"
)

var debug = false

type Response interface {
	Render(w http.ResponseWriter) error
	String() string
}

// Sync response
type syncResponse struct {
	success  bool
	etag     interface{}
	metadata interface{}
	location string
	code     int
	headers  map[string]string
}

func (r *syncResponse) Render(w http.ResponseWriter) error {
	// Set an appropriate ETag header
	if r.etag != nil {
		etag, err := util.EtagHash(r.etag)
		if err == nil {
			w.Header().Set("ETag", etag)
		}
	}

	// Prepare the JSON response
	status := api.Success
	if !r.success {
		status = api.Failure
	}

	if r.headers != nil {
		for h, v := range r.headers {
			w.Header().Set(h, v)
		}
	}

	if r.location != "" {
		w.Header().Set("Location", r.location)
		code := r.code
		if code == 0 {
			code = 201
		}
		w.WriteHeader(code)
	}

	resp := api.ResponseRaw{
		Type:       api.SyncResponse,
		Status:     status.String(),
		StatusCode: int(status),
		Metadata:   r.metadata,
	}

	return util.WriteJSON(w, resp, debug)
}

func (r *syncResponse) String() string {
	if r.success {
		return "success"
	}

	return "failure"
}

func SyncResponse(success bool, metadata interface{}) Response {
	return &syncResponse{success: success, metadata: metadata}
}

var EmptySyncResponse = &syncResponse{success: true, metadata: make(map[string]interface{})}

// Operation response
type operationResponse struct {
	op *operation
}

func (r *operationResponse) Render(w http.ResponseWriter) error {
	_, err := r.op.Run()
	if err != nil {
		return err
	}

	url, md, err := r.op.Render()
	if err != nil {
		return err
	}

	body := api.ResponseRaw{
		Type:       api.AsyncResponse,
		Status:     api.OperationCreated.String(),
		StatusCode: int(api.OperationCreated),
		Operation:  url,
		Metadata:   md,
	}

	w.Header().Set("Location", url)
	w.WriteHeader(202)

	return util.WriteJSON(w, body, debug)
}

func (r *operationResponse) String() string {
	_, md, err := r.op.Render()
	if err != nil {
		return fmt.Sprintf("error: %s", err)
	}

	return md.ID
}

func OperationResponse(op *operation) Response {
	return &operationResponse{op}
}

// Error response
type errorResponse struct {
	code int
	msg  string
}

func (r *errorResponse) String() string {
	return r.msg
}

func (r *errorResponse) Render(w http.ResponseWriter) error {
	var output io.Writer

	buf := &bytes.Buffer{}
	output = buf
	var captured *bytes.Buffer
	if debug {
		captured = &bytes.Buffer{}
		output = io.MultiWriter(buf, captured)
	}

	err := json.NewEncoder(output).Encode(shared.Jmap{"type": api.ErrorResponse, "error": r.msg, "error_code": r.code})

	if err != nil {
		return err
	}

	if debug {
		shared.DebugJson(captured)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(r.code)
	fmt.Fprintln(w, buf.String())

	return nil
}

func BadRequest(err error) Response {
	return &errorResponse{http.StatusBadRequest, err.Error()}
}

func InternalError(err error) Response {
	return &errorResponse{http.StatusInternalServerError, err.Error()}
}

/*
 * SmartError returns the right error message based on err.
 */
func SmartError(err error) Response {
	return EmptySyncResponse
}
