package server

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/stephen/litefs-backup/httputil"
	"github.com/stephen/litefs-backup/store"
)

func Run(ctx context.Context) error {
	store := store.NewStore("XXX")
	return NewServer(store).Open()
}

type Server struct {
	store *store.Store
}

func NewServer(store *store.Store) *Server {
	return &Server{
		store: store,
	}
}

func (s *Server) Open() error {
	r := chi.NewRouter()

	r.Route("/db", func(r chi.Router) {
		// r.Get("/snapshot", httputil.APIHandler(s.handleGetDBSnapshot))
		// r.Get("/info", httputil.APIHandler(s.handleGetDBInfo))
		// r.Get("/page", httputil.APIHandler(s.handleGetDBPage))
		// r.Get("/sync", httputil.APIHandler(s.handleGetDBSync))
		r.Post("/tx", httputil.APIHandler(s.handlePostDBTx))
		// r.Post("/restore", httputil.APIHandler(s.handlePostRestore))
		// r.Post("/upload", httputil.APIHandler(s.handlePostUpload))
	})

	// r.Get("/hwm", httputil.APIHandler(s.handleGetHWM))
	// r.Get("/pos", httputil.APIHandler(s.handleGetPos))
	// r.Get("/info", httputil.APIHandler(s.handleGetInfo))
	// r.Post("/sync", httputil.APIHandler(s.handlePostSync))

	srv := &http.Server{
		Handler: r,
	}

	go srv.ListenAndServe()
	return nil
}
