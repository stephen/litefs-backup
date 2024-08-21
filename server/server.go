package server

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/stephen/litefs-backup/httputil"
	"github.com/stephen/litefs-backup/store"
)

func Run(ctx context.Context) error {
	store := store.NewStore("./data/lfsb")
	if err := store.Open(); err != nil {
		return err
	}

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
		r.Get("/snapshot", httputil.APIHandler(s.handleGetDBSnapshot))
		r.Post("/tx", httputil.APIHandler(s.handlePostDBTx))
		r.Post("/restore", httputil.APIHandler(s.handlePostRestore))
		r.Post("/upload", httputil.APIHandler(s.handlePostUpload))
	})

	r.Get("/pos", httputil.APIHandler(s.handleGetPos))

	addr := ":2200"
	if bind := os.Getenv("LFSB_BIND"); bind != "" {
		addr = bind
	}

	srv := &http.Server{
		Handler: r,
		Addr:    addr,
	}
	slog.Info("server listening", slog.String("addr", addr))

	go srv.ListenAndServe()
	return nil
}
