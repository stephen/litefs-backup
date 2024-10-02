package server

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/getsentry/sentry-go"
	"github.com/go-chi/chi/v5"
	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/httputil"
	"github.com/stephen/litefs-backup/store"
	"github.com/stephen/litefs-backup/store/s3"
	"golang.org/x/sync/errgroup"
)

func Run(ctx context.Context, config *lfsb.Config) (*Server, error) {
	storageClient := s3.NewStorageClient(config)
	if err := storageClient.Open(); err != nil {
		return nil, err
	}

	store := store.NewStore(config)
	store.RemoteClient = storageClient
	if err := store.Open(ctx); err != nil {
		return nil, err
	}

	s := NewServer(config, store)
	if err := s.Open(); err != nil {
		return nil, err
	}
	return s, nil
}

type Server struct {
	config *lfsb.Config
	store  *store.Store

	group  errgroup.Group
	server *http.Server
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
		r.Get("/restore/check", httputil.APIHandler(s.handleGetRestoreCheck))
		r.Post("/upload", httputil.APIHandler(s.handlePostUpload))
		r.Get("/info", httputil.APIHandler(s.handleGetDBInfo))
		r.Delete("/", httputil.APIHandler(s.handleDeleteDB))
	})

	r.Get("/pos", httputil.APIHandler(s.handleGetPos))

	s.server = &http.Server{
		Handler: r,
		Addr:    s.config.Address,
	}
	slog.Info("server listening", slog.String("addr", s.config.Address))

	s.group.Go(func() error {
		defer sentry.Recover()
		return s.server.ListenAndServe()
	})
	return nil
}

func (s *Server) Close() error {
	if s.server != nil {
		if err := s.server.Close(); err != nil {
			return err
		}
	}

	return s.group.Wait()
}
