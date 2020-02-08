package server

import (
	"fmt"
	"github.com/alwaysthanks/xcron/dispatch"
	"github.com/alwaysthanks/xcron/engine"
	"github.com/alwaysthanks/xcron/lib/json"
	"github.com/alwaysthanks/xcron/lib/uuid"
	"github.com/facebookgo/grace/gracehttp"
	"log"
	"net/http"
	"strconv"
	"time"
)

type httpServer struct {
	server *http.Server
}

//server
func NewHttpServer(httpPort int64, engine *engine.Engine) *httpServer {
	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", httpPort),
		Handler:      newHttpRouter(engine),
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
		IdleTimeout:  time.Second * 30,
	}
	return &httpServer{server: server}
}

func (h *httpServer) Run() {
	if err := gracehttp.Serve(h.server); err != nil {
		log.Panicf("http server listen err:%s", err.Error())
	}
}

//adaptor
type httpRouterAdaptor struct {
	engine *engine.Engine
}

func newHttpRouter(engine *engine.Engine) *http.ServeMux {
	adaptor := &httpRouterAdaptor{engine: engine}
	mux := http.NewServeMux()
	//http router path
	mux.HandleFunc("/xcron/createTask", adaptor.wrap(adaptor.createTask))
	return mux
}

//wrap middleware
func (adaptor *httpRouterAdaptor) wrap(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		r.Header.Set(XcronRequestUUId, uuid.GetUUId())
		handler.ServeHTTP(w, r)
	}
}

type httpResponseData struct {
	RequestId string                 `json:"uuid"`
	Code      int                    `json:"code"`
	Message   string                 `json:"message,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
}

// http response
func (adaptor *httpRouterAdaptor) send(resp http.ResponseWriter, respData *httpResponseData) {
	resp.WriteHeader(200)
	if err := json.NewEncoder(resp).Encode(respData); err != nil {
		log.Printf("[error][httpRouterAdaptor.send] could not encode response. respData[%+v],err:%s", *respData, err.Error())
		http.Error(resp, "could not encode response", http.StatusInternalServerError)
	}
}

type createTaskReq struct {
	Type     int          `json:"type"`
	Format   string       `json:"format"`
	Callback taskCallback `json:"callback"`
}

type taskCallback struct {
	Url  string                 `json:"url"`
	Body map[string]interface{} `json:"body"`
}

const (
	//request uuid
	XcronRequestUUId = "Xcron-Request-Uuid"
	//http task type
	httpTaskTypeInstance = 1
	httpTaskTypeCrontab  = 2
)

func (adaptor *httpRouterAdaptor) createTask(w http.ResponseWriter, r *http.Request) {
	requestId := r.Header.Get(XcronRequestUUId)
	var reqTask createTaskReq
	if err := json.NewDecoder(r.Body).Decode(&reqTask); err != nil {
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1000,
			Message:   fmt.Sprintf("parse request params failed:%s", err.Error()),
		})
		return
	}
	switch reqTask.Type {
	case httpTaskTypeInstance:
		adaptor.createInstanceTask(w, requestId, &reqTask)
	case httpTaskTypeCrontab:
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1010,
			Message:   "todo work, not support now...",
		})
	default:
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1020,
			Message:   fmt.Sprintf("not support request task type:%d", reqTask.Type),
		})
	}
}

func (adaptor *httpRouterAdaptor) createInstanceTask(w http.ResponseWriter, requestId string, reqTask *createTaskReq) {
	if len(reqTask.Format) != 10 {
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1001,
			Message:   "instance task time format not unix timestamp",
		})
		return
	}
	timestamp, err := strconv.ParseInt(reqTask.Format, 10, 64)
	if err != nil || timestamp < time.Now().Unix() {
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1002,
			Message:   "instance task time format invalid",
		})
		return
	}
	if reqTask.Callback.Url == "" {
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1003,
			Message:   "instance task time callback url invalid",
		})
		return
	}
	callback := &dispatch.Callback{
		Url:  reqTask.Callback.Url,
		Data: reqTask.Callback.Body,
	}
	taskId, err := adaptor.engine.AddInstanceTask(timestamp, callback)
	if err != nil {
		adaptor.send(w, &httpResponseData{
			RequestId: requestId,
			Code:      1004,
			Message:   "system add instance task failed",
		})
		return
	}
	//success
	adaptor.send(w, &httpResponseData{
		RequestId: requestId,
		Data: map[string]interface{}{
			"task_id": taskId,
		},
	})
}
