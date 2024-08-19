package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/db/sqliteutil"
	"github.com/superfly/ltx"
)

// Txn represents a row in the "txns" table in the data file.
type Txn struct {
	DBID              int
	MinTXID           ltx.TXID
	MaxTXID           ltx.TXID
	PageSize          uint32
	Commit            uint32
	Timestamp         time.Time
	PreApplyChecksum  ltx.Checksum
	PostApplyChecksum ltx.Checksum

	WriteKey       int64
	WriteIndex     int64
	WriteExpiresAt *time.Time
}

// Pending returns true if txn is not yet completed.
func (txn *Txn) Pending() bool {
	return txn.WriteKey != 0
}

// PreApplyPos returns the replication position before the txn is applied.
func (txn *Txn) PreApplyPos() ltx.Pos {
	return ltx.Pos{
		TXID:              txn.MinTXID - 1,
		PostApplyChecksum: ltx.Checksum(txn.PreApplyChecksum),
	}
}

// PostApplyPos returns the replication position after the txn is applied.
func (txn *Txn) PostApplyPos() ltx.Pos {
	return ltx.Pos{
		TXID:              txn.MaxTXID,
		PostApplyChecksum: ltx.Checksum(txn.PostApplyChecksum),
	}
}

func findTxnByMaxTXID(ctx context.Context, tx DBTX, dbID int, maxTXID ltx.TXID) (*Txn, error) {
	var minTXID ltx.TXID
	if err := tx.QueryRowContext(ctx, `
		SELECT min_txid
		FROM txns
		WHERE pending = FALSE AND db_id = ? AND max_txid = ?
		ORDER BY min_txid
	`,
		dbID, maxTXID,
	).Scan(&minTXID); err == sql.ErrNoRows {
		return nil, lfsb.ErrTxNotAvailable
	} else if err != nil {
		return nil, err
	}

	return findTxnByTXIDRange(ctx, tx, dbID, minTXID, maxTXID)
}

func findTxnByMinTXID(ctx context.Context, tx DBTX, dbID int, minTXID ltx.TXID) (*Txn, error) {
	var maxTXID ltx.TXID
	if err := tx.QueryRowContext(ctx, `
		SELECT max_txid
		FROM txns
		WHERE pending = FALSE AND db_id = ? AND min_txid = ?
		ORDER BY max_txid
	`,
		dbID, minTXID,
	).Scan(&maxTXID); err == sql.ErrNoRows {
		return nil, lfsb.ErrTxNotAvailable
	} else if err != nil {
		return nil, err
	}

	return findTxnByTXIDRange(ctx, tx, dbID, minTXID, maxTXID)
}

func findPendingTxnByMinTXID(ctx context.Context, tx DBTX, dbID int, minTXID ltx.TXID) (*Txn, error) {
	var maxTXID ltx.TXID
	if err := tx.QueryRowContext(ctx, `
		SELECT max_txid
		FROM txns
		WHERE pending = TRUE AND db_id = ? AND min_txid = ?
		ORDER BY max_txid
	`,
		dbID, minTXID,
	).Scan(&maxTXID); err == sql.ErrNoRows {
		return nil, lfsb.ErrTxNotAvailable
	} else if err != nil {
		return nil, err
	}

	return findTxnByTXIDRange(ctx, tx, dbID, minTXID, maxTXID)
}

func findTxnByTXIDRange(ctx context.Context, tx DBTX, dbID int, minTXID, maxTXID ltx.TXID) (*Txn, error) {
	txn := Txn{DBID: dbID}

	var writeExpiresAt sqliteutil.NullTime
	if err := tx.QueryRowContext(ctx, `
		SELECT min_txid,
		       max_txid,
		       page_size,
		       "commit",
		       timestamp,
		       pre_apply_checksum,
		       post_apply_checksum,
		       write_key,
		       write_index,
		       write_expires_at
		FROM txns
		WHERE db_id = ? AND min_txid = ? AND max_txid = ?
		ORDER BY min_txid
	`,
		dbID, minTXID, maxTXID,
	).Scan(
		&txn.MinTXID,
		&txn.MaxTXID,
		&txn.PageSize,
		&txn.Commit,
		(*sqliteutil.Time)(&txn.Timestamp),
		(*sqliteutil.Checksum)(&txn.PreApplyChecksum),
		(*sqliteutil.Checksum)(&txn.PostApplyChecksum),
		&txn.WriteKey,
		&txn.WriteIndex,
		&writeExpiresAt,
	); err == sql.ErrNoRows {
		return nil, lfsb.ErrTxNotAvailable
	} else if err != nil {
		return nil, err
	}

	txn.WriteExpiresAt = writeExpiresAt.T()

	return &txn, nil
}

func createTxn(ctx context.Context, tx DBTX, txn *Txn) error {
	// Ensure there are no txns that overlap with the TXID range.
	// This shouldn't happen but we should double check.
	var overlappingN int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM txns
		WHERE db_id = ?
		  AND (min_txid BETWEEN ? AND ? OR max_txid BETWEEN ? AND ?)
	`,
		txn.DBID, txn.MinTXID, txn.MaxTXID, txn.MinTXID, txn.MaxTXID,
	).Scan(
		&overlappingN,
	); err != nil {
		return fmt.Errorf("query overlapping txns: %w", err)
	} else if overlappingN != 0 {
		return fmt.Errorf("overlapping txid range for db %d: %s-%s", txn.DBID, txn.MinTXID, txn.MaxTXID)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO txns (
			db_id,
			min_txid,
			max_txid,
			page_size,
			"commit",
			timestamp,
			pre_apply_checksum,
			post_apply_checksum,
			write_key,
			write_index,
			write_expires_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		txn.DBID,
		txn.MinTXID,
		txn.MaxTXID,
		txn.PageSize,
		txn.Commit,
		sqliteutil.Time(txn.Timestamp),
		sqliteutil.Checksum(txn.PreApplyChecksum),
		sqliteutil.Checksum(txn.PostApplyChecksum),
		txn.WriteKey,
		txn.WriteIndex,
		sqliteutil.NewNullTime(txn.WriteExpiresAt),
	); err != nil {
		return err
	}
	return nil
}

func deletePendingTxnAndPages(ctx context.Context, tx DBTX, dbID int, minTXID, maxTXID ltx.TXID) error {
	if result, err := tx.ExecContext(ctx, `
		DELETE FROM txns
		WHERE db_id = ? AND min_txid = ? AND max_txid = ?
	`,
		dbID,
		minTXID,
		maxTXID,
	); err != nil {
		return err
	} else if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return lfsb.ErrTxNotAvailable
	}

	if _, err := tx.ExecContext(ctx, `
		DELETE FROM pages
		WHERE db_id = ? AND min_txid = ? AND max_txid = ?
	`,
		dbID,
		minTXID,
		maxTXID,
	); err != nil {
		return fmt.Errorf("delete pages: %w", err)
	}
	return nil
}
