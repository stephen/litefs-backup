package server

import (
	"context"
	"io"
	"net/http"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/httputil"
	"github.com/stephen/litefs-backup/store"
	"github.com/superfly/ltx"
)

func (s *Server) handlePostDBTx(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	cluster := httputil.ClusterNameFromContext(ctx)

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
	cluster := httputil.ClusterNameFromContext(ctx)

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

func (s *Server) handleGetDBSnapshot(ctx context.Context, w http.ResponseWriter, r *http.Request) (err error) {
	q := r.URL.Query()
	cluster := httputil.ClusterNameFromContext(ctx)

	name := q.Get("db")
	if err := lfsb.ValidateDatabase(name); err != nil {
		return err
	}

	// Determine the current replication position of the database.
	db, err := s.store.FindDBByName(r.Context(), cluster, name)
	if err != nil {
		return err
	}

	// Generate a snapshot up to the TXID of the read position.
	w.Header().Set("Content-Type", "application/octet-stream")
	switch q.Get("format") {
	case "", "ltx":
		return s.store.WriteSnapshotTo(ctx, cluster, name, db.TXID, w)
	case "sqlite":
		return s.store.WriteDatabaseTo(ctx, cluster, name, db.TXID, w)
	default:
		return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADFORMAT", "unsupported snapshot format")
	}
}

func (s *Server) handleGetRestoreCheck(ctx context.Context, w http.ResponseWriter, r *http.Request) (err error) {
	q := r.URL.Query()
	cluster := httputil.ClusterNameFromContext(ctx)

	name := q.Get("db")
	if err := lfsb.ValidateDatabase(name); err != nil {
		return err
	}

	var txID ltx.TXID
	if txIDStr := q.Get("txid"); txIDStr != "" {
		if txID, err = ltx.ParseTXID(q.Get("txid")); err != nil {
			return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "invalid txid")
		}
	}

	var timestamp time.Time
	if timestampStr := q.Get("timestamp"); timestampStr != "" {
		if timestamp, err = ltx.ParseTimestamp(timestampStr); err != nil {
			return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "invalid timestamp")
		}
	}

	if txID != 0 && !timestamp.IsZero() {
		return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "can't specify both 'txid' and 'timestamp'")
	} else if txID == 0 && timestamp.IsZero() {
		return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "'txid' or 'timestamp' required")
	}

	var path store.StoragePath
	if !timestamp.IsZero() {
		foundPath, err := s.store.FindStoragePathByTimestamp(ctx, cluster, name, timestamp)
		if err != nil {
			return err
		}
		path = foundPath
		txID = path.MaxTXID
	} else {
		paths, err := store.CalcSnapshotPlan(ctx, s.store.RemoteClient, cluster, name, txID)
		if err != nil {
			return err
		}
		path = paths[len(paths)-1]
	}

	if path.IsZero() {
		return lfsb.ErrTxNotAvailable
	}

	md, err := s.store.RemoteClient.Metadata(ctx, path)
	if err != nil {
		return err
	}

	// Attempt to write out the snapshot to verify this timestamp/txid is ok.
	if err := s.store.WriteSnapshotTo(ctx, cluster, name, txID, io.Discard); err != nil {
		return err
	}

	return httputil.RenderResponse(w, getRestoreCheckResponse{TXID: txID, Timestamp: md.Timestamp})
}

type getRestoreCheckResponse struct {
	Timestamp time.Time `json:"timestamp"`
	TXID      ltx.TXID  `json:"txID"`
}

func (s *Server) handlePostRestore(ctx context.Context, w http.ResponseWriter, r *http.Request) (err error) {
	q := r.URL.Query()
	cluster := httputil.ClusterNameFromContext(ctx)

	name := q.Get("db")
	if err := lfsb.ValidateDatabase(name); err != nil {
		return err
	}

	var txID ltx.TXID
	if txIDStr := q.Get("txid"); txIDStr != "" {
		if txID, err = ltx.ParseTXID(q.Get("txid")); err != nil {
			return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "invalid txid")
		}
	}

	var timestamp time.Time
	if timestampStr := q.Get("timestamp"); timestampStr != "" {
		if timestamp, err = ltx.ParseTimestamp(timestampStr); err != nil {
			return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "invalid timestamp")
		}
	}

	var newTX ltx.TXID
	if txID != 0 && !timestamp.IsZero() {
		return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "can't specify both 'txid' and 'timestamp'")
	} else if txID == 0 && timestamp.IsZero() {
		return lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADINPUT", "'txid' or 'timestamp' required")
	}

	if !timestamp.IsZero() {
		txID, err = s.store.FindTXIDByTimestamp(ctx, cluster, name, timestamp)
		if err != nil {
			return err
		}
	}

	newTX, err = s.store.RestoreToTx(ctx, cluster, name, txID)
	if err != nil {
		return err
	}

	return httputil.RenderResponse(w, postRestoreResponse{TXID: newTX})
}

type postRestoreResponse struct {
	TXID ltx.TXID `json:"txID"`
}

func (s *Server) handlePostUpload(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	cluster := httputil.ClusterNameFromContext(ctx)

	name := q.Get("db")
	if err := lfsb.ValidateDatabase(name); err != nil {
		return err
	}

	pos, err := s.store.StoreDatabase(ctx, cluster, name, r.Body)
	if err != nil {
		return err
	}

	return httputil.RenderResponse(w, postUploadResponse{TXID: pos.TXID})
}

type postUploadResponse struct {
	TXID ltx.TXID `json:"txID"`
}

func (s *Server) handleGetDBInfo(ctx context.Context, w http.ResponseWriter, r *http.Request) (err error) {
	q := r.URL.Query()
	cluster := httputil.ClusterNameFromContext(ctx)

	name := q.Get("db")
	if err := lfsb.ValidateDatabase(name); err != nil {
		return err
	}
	_, err = s.store.FindDBByName(ctx, cluster, name)
	if err != nil {
		return err
	}

	info, err := s.store.Info(ctx, cluster, name, q.Has("all"))
	if err != nil {
		return err
	}

	paths := make([]getDBInfoResponsePath, 0, len(info.RestorablePaths))
	for _, p := range info.RestorablePaths {
		paths = append(paths, getDBInfoResponsePath{
			MaxTXID:           p.MaxTXID.String(),
			Timestamp:         &p.Metadata.Timestamp,
			PostApplyChecksum: p.Metadata.PostApplyChecksum.String(),
		})
	}
	resp := getDBInfoResponse{Name: name, RestorablePaths: paths}

	return httputil.RenderResponse(w, resp)
}

type getDBInfoResponse struct {
	Name            string                  `json:"name"`
	RestorablePaths []getDBInfoResponsePath `json:"restorablePaths"`
}

type getDBInfoResponsePath struct {
	MaxTXID           string     `json:"maxTxId"`
	Timestamp         *time.Time `json:"maxTimestamp,omitempty"`
	PostApplyChecksum string     `json:"postApplyChecksum"`
}

func (s *Server) handleDeleteDB(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	cluster := httputil.ClusterNameFromContext(ctx)
	if err := lfsb.ValidateClusterName(cluster); err != nil {
		return err
	}

	name := q.Get("db")
	if err := lfsb.ValidateDatabase(name); err != nil {
		return err
	}
	if _, err := s.store.FindDBByName(ctx, cluster, name); err != nil {
		return err
	}

	return s.store.DropDB(ctx, cluster, name)
}
