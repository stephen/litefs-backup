package server

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/httputil"
	"github.com/stephen/litefs-backup/store"
)

func Run(ctx context.Context, config *lfsb.Config) error {
	store := store.NewStore(config)
	if err := store.Open(); err != nil {
		return err
	}

	return NewServer(config, store).Open()
}

type Server struct {
	config *lfsb.Config
	store  *store.Store
}

func NewServer(config *lfsb.Config, store *store.Store) *Server {
	return &Server{
		config: config,
		store:  store,
	}
}

func (s *Server) Open() error {
	r := chi.NewRouter()
	r.Use(httputil.CheckClusterNoAuth)

	r.Route("/db", func(r chi.Router) {
		r.Get("/snapshot", httputil.APIHandler(s.handleGetDBSnapshot))
		r.Post("/tx", httputil.APIHandler(s.handlePostDBTx))
		r.Post("/restore", httputil.APIHandler(s.handlePostRestore))
		r.Post("/upload", httputil.APIHandler(s.handlePostUpload))
	})

	r.Get("/pos", httputil.APIHandler(s.handleGetPos))

	srv := &http.Server{
		Handler: r,
		Addr:    s.config.Address,
	}
	slog.Info("server listening", slog.String("addr", s.config.Address))

	go srv.ListenAndServe()
	return nil
}
