package store

import (
	"context"
	"io"
	"os"

	lfsb "github.com/stephen/litefs-backup"

	"github.com/superfly/ltx"
)

// WriteSnapshotTo writes the snapshot at a given TXID to w.
func (s *Store) WriteSnapshotTo(ctx context.Context, cluster, database string, txID ltx.TXID, w io.Writer) error {
	if err := s.WriteLocalSnapshotTo(ctx, cluster, database, txID, w); lfsb.ErrorCode(err) == "ENOTXID" {
		return s.WriteRemoteSnapshotTo(ctx, cluster, database, txID, w)
	} else if err != nil {
		return err
	}
	return nil
}

// WriteLocalSnapshotTo attempts to read the snapshot out from
// the local sqlite store.
func (s *Store) WriteLocalSnapshotTo(ctx context.Context, cluster, database string, txID ltx.TXID, w io.Writer) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	db, err := findDBByName(ctx, tx, cluster, database)
	if err != nil {
		return err
	}
	return writeSnapshotTo(ctx, tx, db.ID, txID, w)
}

// WriteDatabaseTo writes a database decoded from an LTX snapshot to the given writer.
func (s *Store) WriteDatabaseTo(ctx context.Context, cluster, name string, txID ltx.TXID, w io.Writer) error {
	tempFile, err := s.createTemp("lfsc-write-database-to-*.ltx")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()
	defer func() { _ = tempFile.Close() }()

	// Write a snapshot to a temp file so that we can release the database transaction quickly.
	if err := s.WriteSnapshotTo(ctx, cluster, name, txID, tempFile); err != nil {
		return err
	}

	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return ltx.NewDecoder(tempFile).DecodeDatabaseTo(w)
}

// WriteRemoteSnapshotTo writes a snapshot (from remote storage only) at a given TXID to w.
func (s *Store) WriteRemoteSnapshotTo(ctx context.Context, cluster, name string, txID ltx.TXID, w io.Writer) error {
	paths, err := CalcSnapshotPlan(ctx, s.RemoteClient, cluster, name, txID)
	if err != nil {
		return err
	}

	c, close, err := NewCompactorFromPaths(ctx, s.RemoteClient, paths, w)
	if err != nil {
		return err
	}
	defer func() { _ = close() }()

	if err := c.Compact(ctx); err != nil {
		return err
	}
	return close()
}
