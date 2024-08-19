package server

import (
	"context"
	"net/http"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/httputil"
	"github.com/superfly/ltx"
)

func (s *Server) handlePostDBTx(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	// cluster := lfsc.ClusterNameFromContext(ctx)
	cluster := "XXX"

	name := r.URL.Query().Get("db")
	if err := lfsb.ValidateDatabase(name); err != nil {
		return err
	}

	if _, err := s.store.WriteTx(ctx, cluster, name, r.Body, nil); err != nil {
		return err
	}

	// Return the high-water mark of the position the database has been replicated up to.
	db, err := s.store.FindDBByName(ctx, cluster, name)
	if err != nil {
		return err
	}
	w.Header().Set("Litefs-Hwm", db.HWM.String())

	return nil
}

func (s *Server) handleGetPos(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cluster := "XXX"

	// Determine the current replication position for all databases.
	dbs, err := s.store.FindDBsByCluster(ctx, cluster)
	if err != nil {
		return err
	}

	m := make(map[string]ltx.Pos)
	for _, db := range dbs {
		m[db.Name] = db.Pos()
	}

	return httputil.RenderResponse(w, m)
}
