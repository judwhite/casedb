package main

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/judwhite/httplog"
	log "github.com/judwhite/logrjack"
)

func (svr *webServer) Start() error {
	r := mux.NewRouter()

	r.NotFoundHandler = svr

	r.HandleFunc("/ping", svr.handler("/api/v1/ping", svr.pingHandler))
	r.HandleFunc("/stats", svr.handler("/api/v1/stats", svr.statsHandler))

	apiv1 := r.PathPrefix("/api/v1").Subrouter()
	apiv1.HandleFunc("/ping", svr.handler("/api/v1/ping", svr.pingHandler))
	apiv1.HandleFunc("/stats", svr.handler("/api/v1/stats", svr.statsHandler))

	attachProfiler(r)

	if err := svr.raftServer.Start(); err != nil {
		return err
	}

	http.Handle("/", r)

	log.Infof("listening on %s...", settings.HTTPListenAddress)
	go func() {
		log.Fatal(http.ListenAndServe(settings.HTTPListenAddress, nil))
	}()

	return nil
}

//type loggedHandler func(r *http.Request, requestLogger httplog.Entry) (httplog.Response, error)

func (svr *webServer) handler(name string, h func(r *http.Request, requestLogger httplog.Entry) (httplog.Response, error)) func(w http.ResponseWriter, r *http.Request) {
	return svr.Handle(httplog.Handler{Name: name, Func: h})
}

func (svr *webServer) Stop() {
	log.Info("stopping web server...")
	svr.Shutdown()
	log.Info("web server stopped.")
	svr.raftServer.Stop()
}

func (svr *webServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f := func(_ *http.Request, _ httplog.Entry) (httplog.Response, error) {
		return httplog.Response{Body: "404 page not found", Status: http.StatusNotFound}, nil
	}

	handler := svr.handler("route-not-found", f)
	handler(w, r)
}

func (svr *webServer) pingHandler(r *http.Request, requestLogger httplog.Entry) (httplog.Response, error) {
	// TODO (judwhite)
	response := "OK"
	responseCode := http.StatusOK
	return httplog.Response{Body: response, Status: responseCode}, nil
}

func (svr *webServer) statsHandler(r *http.Request, requestLogger httplog.Entry) (httplog.Response, error) {
	// TODO (judwhite)
	return httplog.Response{Body: "TODO"}, nil
}

func attachProfiler(r *mux.Router) {
	r.PathPrefix("/debug/pprof").Handler(http.DefaultServeMux)
}
