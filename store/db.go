package store

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/db/sqliteutil"
	"github.com/superfly/ltx"
)

type DBTX interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

// DB represents a database row in the shard.
type DB struct {
	ID                int
	Cluster           string
	Name              string
	HWM               ltx.TXID
	TXID              ltx.TXID     // derived field
	PostApplyChecksum ltx.Checksum // derived field
	PageSize          uint32       // derived field
	Commit            uint32       // derived field
	Timestamp         time.Time    // derived field
}

func (db *DB) Pos() ltx.Pos {
	return ltx.Pos{
		TXID:              db.TXID,
		PostApplyChecksum: db.PostApplyChecksum,
	}
}

func findDB(ctx context.Context, tx DBTX, id int) (*DB, error) {
	var db DB
	if err := tx.QueryRowContext(ctx, `
		SELECT d.id, d.cluster, d.name, d.hwm, IFNULL(t.max_txid,0), t.post_apply_checksum, IFNULL(t.page_size,0), IFNULL(t."commit",0), t.timestamp
		FROM databases d
		LEFT JOIN txns t ON d.id = t.db_id
		WHERE d.id = ?
		ORDER BY t.max_txid DESC
		LIMIT 1
	`,
		id,
	).Scan(
		&db.ID,
		&db.Cluster,
		&db.Name,
		&db.HWM,
		&db.TXID,
		(*sqliteutil.Checksum)(&db.PostApplyChecksum),
		&db.PageSize,
		&db.Commit,
		(*sqliteutil.Time)(&db.Timestamp),
	); err == sql.ErrNoRows {
		return nil, lfsb.ErrDatabaseNotFound
	} else if err != nil {
		return nil, err
	}
	return &db, nil
}

func findDBByName(ctx context.Context, tx DBTX, cluster, name string) (*DB, error) {
	var id int
	if err := tx.QueryRowContext(ctx, `
		SELECT id
		FROM databases
		WHERE cluster = ? AND name = ?
	`,
		cluster, name,
	).Scan(
		&id,
	); err == sql.ErrNoRows {
		return nil, lfsb.ErrDatabaseNotFound
	} else if err != nil {
		return nil, err
	}
	return findDB(ctx, tx, id)
}

func createDBIfNotExist(ctx context.Context, tx DBTX, db *DB) error {
	other, err := findDBByName(ctx, tx, db.Cluster, db.Name)
	if err == lfsb.ErrDatabaseNotFound {
		return createDB(ctx, tx, db)
	} else if err != nil {
		return err
	}
	*db = *other
	return nil
}

func createDB(ctx context.Context, tx DBTX, db *DB) error {
	return tx.QueryRowContext(ctx, `
		INSERT INTO databases (cluster, name)
		VALUES (?, ?)
		RETURNING id
	`,
		db.Cluster,
		db.Name,
	).Scan(
		&db.ID,
	)
}

func deleteDB(ctx context.Context, tx DBTX, id int) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM databases WHERE id = ?`, id)
	return err
}

// increaseHWM updates the high-water mark if the new value is higher.
func increaseHWM(ctx context.Context, tx DBTX, dbID int, hwm ltx.TXID) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE databases
		SET hwm = ?
		WHERE id = ?
		  AND hwm < ?
	`,
		hwm, dbID, hwm,
	)
	return err
}

func findDBsByCluster(ctx context.Context, tx DBTX, cluster string) ([]*DB, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM databases
		WHERE cluster = ?
		ORDER BY name
	`,
		cluster,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var a []*DB
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		db, err := findDB(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		a = append(a, db)
	}

	if err := rows.Err(); err != nil {
		return a, err
	}

	return a, rows.Close()
}

func findClusters(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT cluster
		FROM databases
		ORDER BY cluster
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var a []string
	for rows.Next() {
		var cluster string
		if err := rows.Scan(&cluster); err != nil {
			return nil, err
		}
		a = append(a, cluster)
	}

	if err := rows.Err(); err != nil {
		return a, err
	}
	return a, rows.Close()
}

func writeSnapshotTo(ctx context.Context, tx DBTX, dbID int, txID ltx.TXID, w io.Writer) error {
	enc := ltx.NewEncoder(w)

	txn, err := findTxnByMaxTXID(ctx, tx, dbID, txID)
	if err != nil {
		return err
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		Flags:     ltx.HeaderFlagCompressLZ4,
		MinTXID:   1,
		MaxTXID:   txn.MaxTXID,
		PageSize:  txn.PageSize,
		Commit:    txn.Commit,
		Timestamp: txn.Timestamp.UnixMilli(),
	}); err != nil {
		return fmt.Errorf("encode ltx header: %w", err)
	}

	postApplyChecksum, err := encodeSnapshotPagesTo(ctx, tx, dbID, txID, enc)
	if err != nil {
		return err
	} else if txn.PostApplyChecksum != postApplyChecksum {
		return fmt.Errorf("post-apply checksum mismatch: stored=%016x calculated=%016x", txn.PostApplyChecksum, postApplyChecksum)
	}

	enc.SetPostApplyChecksum(postApplyChecksum)
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close ltx encoder: %w", err)
	}
	return nil
}
