package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/facebookgo/grace/gracehttp"
	"github.com/robustirc/rafthttp"
	"global"
	httpclient "lib/http"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type HttpTransport struct {
	*rafthttp.HTTPTransport
	addr   string
	engine *Engine
	logger *log.Logger
}

func NewHTTPTransport(engine *Engine, addr string, logger *log.Logger) *HttpTransport {
	if logger == nil {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	//http raft router:
	// /xcron/raft/InstallSnapshot
	// /xcron/raft/InstallSnapshotStreaming
	// /xcron/raft/RequestVote
	// /xcron/raft/AppendEntries
	raftTrans := rafthttp.NewHTTPTransport(addr, nil, logger, "http://%s/xcron/raft/")
	httpTrans := &HttpTransport{
		addr:          addr,
		engine:        engine,
		logger:        logger,
		HTTPTransport: raftTrans,
	}
	return httpTrans
}

func (t *HttpTransport) Start() {
	//all peers router
	mux := http.NewServeMux()
	mux.Handle("/xcron/", t)
	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", t.addr),
		Handler:      mux,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		IdleTimeout:  time.Second * 10,
	}
	go func() {
		if err := gracehttp.Serve(server); err != nil {
			t.logger.Panicf("[panic][HttpTransport.Start] err:%s", err.Error())
		}
	}()
	go t.heartbeat()
}

// ServeHTTP implements the net/http.Handler interface
func (t *HttpTransport) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	router := req.URL.Path
	if router == "/xcron/peer" {
		t.servePeer(resp, req)
	} else if router == "/xcron/group" {
		t.serveGroup(resp, req)
	} else if strings.HasPrefix(router, "/xcron/raft/") {
		//raft transport
		t.HTTPTransport.ServeHTTP(resp, req)
	} else {
		//not found
		http.Error(resp, fmt.Sprintf("not found router %s", router), 404)
	}
}

type TransportRequest struct {
	Cmd  string `json:"cmd"`
	Data string `json:"data"`
}

type TransportResponse struct {
	Code  int    `json:"code"`
	Error string `json:"error,omitempty"`
	Data  string `json:"data,omitempty"`
}

func (t *HttpTransport) serveGroup(resp http.ResponseWriter, req *http.Request) {

}

const (
	transportCodePeerParseErr = 1001
	transportCodePeerStoreErr = 1002
	transportCodePeerJoinErr  = 1003
	transportCodePeerIncrErr  = 1004
)

func (t *HttpTransport) servePeer(resp http.ResponseWriter, req *http.Request) {
	//decode req
	var transReq TransportRequest
	if err := json.NewDecoder(req.Body).Decode(&transReq); err != nil {
		t.logger.Printf("[error][HttpTransport.servePeer] ReadAll req.Body err:%s", err.Error())
		t.send(resp, &TransportResponse{Code: transportCodePeerParseErr, Error: "parse request error"})
		return
	}
	//process
	var transResp = new(TransportResponse)
	switch transReq.Cmd {
	case "store":
		if err := t.engine.distributeCmd("store", []byte(transReq.Data)); err != nil {
			t.logger.Printf("[error][HttpTransport.servePeer] engine distributeCmd error. data:%s, err:%s", transReq.Data, err.Error())
			transResp = &TransportResponse{Error: "store error", Code: transportCodePeerStoreErr}
		}
	case "join":
		if err := t.engine.Join(transReq.Data); err != nil {
			t.logger.Printf("[error][HttpTransport.servePeer] engine Join error. data:%s, err:%s", transReq.Data, err.Error())
			transResp = &TransportResponse{Error: "join error", Code: transportCodePeerJoinErr}
		}
	case "incr":
		ret, err := t.engine.distributeAtomIncr(transReq.Data)
		if err != nil {
			t.logger.Printf("[error][HttpTransport.servePeer] engine distributeAtomIncr error. data:%s, err:%s", transReq.Data, err.Error())
			transResp = &TransportResponse{Error: "incr error", Code: transportCodePeerIncrErr}
		}
		transResp = &TransportResponse{Data: strconv.FormatInt(ret, 10)}
	case "heartbeat":
		t.engine.activePeers.Add(transReq.Data)
	}
	t.send(resp, transResp)
}

func (t *HttpTransport) send(resp http.ResponseWriter, respData *TransportResponse) {
	resp.WriteHeader(200)
	if err := json.NewEncoder(resp).Encode(respData); err != nil {
		t.logger.Printf("[error][HttpTransport.send] could not encode response. resp[code:%d,err:%s,data:%s],err:%s", respData.Code, respData.Error, respData.Data, err.Error())
		http.Error(resp, "could not encode response", http.StatusInternalServerError)
	}
}

func (t *HttpTransport) heartbeat() {
	client := httpclient.NewHttpClient(3, 5)
	localAddr := global.XcronState.GetLocalAddr()
	request := TransportRequest{
		Cmd:  "heartbeat",
		Data: localAddr,
	}
	body, _ := json.Marshal(&request)
	for range time.Tick(time.Second * 30) {
		//add self
		t.engine.activePeers.Add(localAddr)
		//curl peers
		peers := global.XcronConf.GetPeers()
		for _, host := range peers {
			url := fmt.Sprintf("http://%s/xcron/peer", host)
			//todo use routine pool
			//todo exclude self host
			go func(url string) {
				if _, err := client.Post(url, bytes.NewReader(body)); err != nil {
					t.logger.Printf("[error][HttpTransport.heartbeat] error. reqUrl:%s, reqBody:%s, respErr:%s", url, string(body), err.Error())
				}
			}(url)
		}
	}

	global.XcronConf.GetPeers()
}
