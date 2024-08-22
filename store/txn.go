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
		WHERE db_id = ? AND max_txid = ?
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
		WHERE db_id = ? AND min_txid = ?
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

	if err := tx.QueryRowContext(ctx, `
		SELECT min_txid,
		       max_txid,
		       page_size,
		       "commit",
		       timestamp,
		       pre_apply_checksum,
		       post_apply_checksum
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
	); err == sql.ErrNoRows {
		return nil, lfsb.ErrTxNotAvailable
	} else if err != nil {
		return nil, err
	}

	return &txn, nil
}

func findMaxTxnBeforeTimestamp(ctx context.Context, tx *sql.Tx, dbID int, timestamp time.Time) (*Txn, error) {
	var minTXID, maxTXID ltx.TXID
	if err := tx.QueryRowContext(ctx, `
		SELECT min_txid, max_txid
		FROM txns
		WHERE db_id = ? AND timestamp < ?
		ORDER BY max_txid DESC
		LIMIT 1
	`,
		dbID, ltx.FormatTimestamp(timestamp),
	).Scan(
		&minTXID, &maxTXID,
	); err == sql.ErrNoRows {
		return nil, lfsb.ErrTxNotAvailable
	} else if err != nil {
		return nil, err
	}

	return findTxnByTXIDRange(ctx, tx, dbID, minTXID, maxTXID)
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
			post_apply_checksum
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`,
		txn.DBID,
		txn.MinTXID,
		txn.MaxTXID,
		txn.PageSize,
		txn.Commit,
		sqliteutil.Time(txn.Timestamp),
		sqliteutil.Checksum(txn.PreApplyChecksum),
		sqliteutil.Checksum(txn.PostApplyChecksum),
	); err != nil {
		return err
	}
	return nil
}

func deleteTxnsBeforeMaxTXID(ctx context.Context, tx *sql.Tx, dbID int, maxTXID ltx.TXID) error {
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM txns
		WHERE db_id = ?
		  AND max_txid < ?
	`,
		dbID,
		maxTXID,
	); err != nil {
		return err
	}

	// Ensure that at least one txn still exists.
	var n int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM txns WHERE db_id = ?`, dbID).Scan(&n); err != nil {
		return fmt.Errorf("count txns: %w", err)
	} else if n == 0 {
		return fmt.Errorf("cannot delete all txns for db #%d", dbID)
	}

	return nil
}
