package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"

	websocket "github.com/gorilla/websocket"
	jsonrpc2 "github.com/sourcegraph/jsonrpc2"
	rdf "github.com/underlay/go-rdfjs"
	styx "github.com/underlay/styx"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return r.URL.Host == ""
	},
	Subprotocols: []string{"rpc"},
}

func handleRPC(w http.ResponseWriter, r *http.Request, store *styx.Store) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	ctx := context.Background()
	stream := &jsonObjectStream{conn}
	handler := &rpcHandler{store: store}
	c := jsonrpc2.NewConn(ctx, stream, handler)
	<-c.DisconnectNotify()
	if handler.iter != nil {
		handler.iter.Close()
		handler.iter = nil
	}
}

type method func(params []json.RawMessage, store *styx.Store, handler *rpcHandler) (interface{}, int64, error)

var methods = map[string]method{
	"query": callQuery,
	"next":  callNext,
	"seek":  callSeek,
	"prov":  callProv,
	"close": callClose,
}

func callQuery(params []json.RawMessage, store *styx.Store, handler *rpcHandler) (interface{}, int64, error) {
	if len(params) == 0 || len(params) > 3 {
		return nil, jsonrpc2.CodeInvalidParams, nil
	}

	quads := make([]*rdf.Quad, 0)
	err := json.Unmarshal(params[0], &quads)
	if err != nil || len(quads) == 0 {
		return nil, jsonrpc2.CodeInvalidParams, err
	}

	var domain []rdf.Term
	if len(params) > 1 {
		domain, err = rdf.UnmarshalTerms(params[1])
		if err != nil {
			return nil, jsonrpc2.CodeInvalidParams, err
		}
	}

	var index []rdf.Term
	if len(params) > 2 {
		index, err = rdf.UnmarshalTerms(params[2])
		if err != nil {
			return nil, jsonrpc2.CodeInvalidParams, err
		}
	}

	handler.iter, err = store.Query(quads, domain, index)
	if err != nil {
		return nil, jsonrpc2.CodeInternalError, err
	}

	return handler.iter.Domain(), 0, nil
}

func callClose(params []json.RawMessage, store *styx.Store, handler *rpcHandler) (interface{}, int64, error) {
	if handler.iter == nil {
		return nil, jsonrpc2.CodeInvalidRequest, nil
	}

	if len(params) > 0 {
		return nil, jsonrpc2.CodeInvalidParams, nil
	}

	handler.iter.Close()
	handler.iter = nil
	return nil, 0, nil
}

func callNext(params []json.RawMessage, store *styx.Store, handler *rpcHandler) (interface{}, int64, error) {
	if handler.iter == nil {
		return nil, jsonrpc2.CodeInvalidRequest, nil
	}

	if len(params) > 1 {
		return nil, jsonrpc2.CodeInvalidParams, nil
	}

	var err error
	var term rdf.Term
	if len(params) > 0 {
		term, err = rdf.UnmarshalTerm(params[0])
		if err != nil {
			return nil, jsonrpc2.CodeInvalidParams, nil
		}

		t := term.TermType()
		if t != rdf.BlankNodeType && t != rdf.VariableType {
			return nil, jsonrpc2.CodeInvalidParams, nil
		}
	}

	delta, err := handler.iter.Next(term)

	if err != nil {
		return nil, jsonrpc2.CodeInternalError, err
	}

	return delta, 0, nil
}

func callSeek(params []json.RawMessage, store *styx.Store, handler *rpcHandler) (interface{}, int64, error) {
	if handler.iter == nil {
		return nil, jsonrpc2.CodeInvalidRequest, nil
	}

	if len(params) > 1 {
		return nil, jsonrpc2.CodeInvalidParams, nil
	}

	var index []rdf.Term
	var err error
	if len(params) > 0 {
		index, err = rdf.UnmarshalTerms(params[0])
		if err != nil {
			return nil, jsonrpc2.CodeInvalidParams, err
		}
	}

	err = handler.iter.Seek(index)
	if err != nil {
		return nil, jsonrpc2.CodeInternalError, err
	}

	return nil, 0, nil
}

func callProv(params []json.RawMessage, store *styx.Store, handler *rpcHandler) (interface{}, int64, error) {
	if handler.iter == nil {
		return nil, jsonrpc2.CodeInvalidRequest, nil
	}

	if len(params) > 0 {
		return nil, jsonrpc2.CodeInvalidParams, nil
	}

	prov, err := handler.iter.Prov()
	if err != nil {
		return nil, jsonrpc2.CodeInternalError, err
	}

	return prov, 0, nil
}

type rpcHandler struct {
	store *styx.Store
	iter  *styx.Iterator
}

func (handler *rpcHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, request *jsonrpc2.Request) {
	var result interface{}
	var code int64
	var err error

	if method, has := methods[request.Method]; !has {
		code = jsonrpc2.CodeMethodNotFound
	} else {
		params := make([]json.RawMessage, 0)
		if request.Params != nil {
			err = json.Unmarshal(*request.Params, &params)
			if err != nil {
				code = jsonrpc2.CodeInvalidParams
			}
		}

		if code == 0 && err == nil {
			result, code, err = method(params, handler.store, handler)
		}
	}

	if code != 0 {
		respErr := &jsonrpc2.Error{Code: code}
		if err != nil {
			respErr.Message = err.Error()
		}
		_ = conn.ReplyWithError(ctx, request.ID, respErr)
	} else {
		conn.Reply(ctx, request.ID, result)
	}
}

type jsonObjectStream struct {
	conn *websocket.Conn
}

func (os *jsonObjectStream) Close() error {
	return os.conn.Close()
}

// WriteObject writes a JSON object to the stream
func (os *jsonObjectStream) WriteObject(obj interface{}) error {
	return os.conn.WriteJSON(obj)
}

// ReadObject reads a JSON object from the stream
func (os *jsonObjectStream) ReadObject(v interface{}) error {
	err := os.conn.ReadJSON(v)
	if err != nil && websocket.IsCloseError(err, 1000, 1001, 1005) {
		return io.EOF // 😎
	}
	return err
}
