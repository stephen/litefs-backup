package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/db/sqliteutil"
	"github.com/superfly/ltx"
)

// Page returns a page that is valid between a transaction range.
type Page struct {
	DBID    int
	Pgno    uint32
	MinTXID ltx.TXID
	MaxTXID ltx.TXID
	Chksum  uint64
	Nonce   []byte
	Tag     []byte
	Data    []byte
}

// findPageAtTXID returns the page state at a given transaction ID.
// It is the responsibility of the caller to verify that the txn exists, if needed.
func findPageAtTXID(ctx context.Context, tx *sql.Tx, dbID int, txID ltx.TXID, pgno uint32) (*Page, error) {
	page := Page{
		DBID: dbID,
		Pgno: pgno,
	}

	if err := tx.QueryRow(`
		SELECT min_txid, max_txid, chksum, data
		FROM pages
		WHERE db_id = ? AND min_txid <= ? AND pgno = ?
		ORDER BY min_txid DESC
		LIMIT 1
	`,
		dbID, txID, pgno,
	).Scan(
		&page.MinTXID,
		&page.MaxTXID,
		(*sqliteutil.Checksum)(&page.Chksum),
		&page.Data,
	); err == sql.ErrNoRows {
		return nil, lfsb.ErrPageNotFound
	} else if err != nil {
		return nil, err
	}
	return &page, nil
}

// findPagesChangedSinceTXID returns a list of numbers of the pages that have changed since txn.
// It is the responsibility of the caller to verify that the txn exists, if needed.
func findPagesChangedSinceTXID(ctx context.Context, tx *sql.Tx, dbID int, txID ltx.TXID) ([]uint32, error) {
	rows, err := tx.Query(`
		SELECT DISTINCT pgno
		FROM pages
		WHERE db_id = ? AND max_txid > ?
		ORDER BY pgno
	`, dbID, txID)
	if err != nil {
		return nil, fmt.Errorf("find changed pages: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var pgnos []uint32
	for rows.Next() {
		var pgno uint32
		if err := rows.Scan(&pgno); err != nil {
			return nil, err
		}

		pgnos = append(pgnos, pgno)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return pgnos, rows.Close()
}

func writeLTXPageRangeTo(ctx context.Context, tx *sql.Tx, db *DB, minTXID, maxTXID ltx.TXID, w io.Writer) (ltx.Header, ltx.Trailer, error) {
	minTxn, err := findTxnByMinTXID(ctx, tx, db.ID, minTXID)
	if err != nil {
		return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("fetch min txn @ %s: %w", minTXID, err)
	}

	maxTxn, err := findTxnByMaxTXID(ctx, tx, db.ID, maxTXID)
	if err != nil {
		return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("fetch max txn @ %s: %w", maxTXID, err)
	}

	enc := ltx.NewEncoder(w)
	if err := enc.EncodeHeader(ltx.Header{
		Version:          ltx.Version,
		Flags:            ltx.HeaderFlagCompressLZ4,
		PageSize:         db.PageSize,
		Commit:           maxTxn.Commit,
		MinTXID:          minTXID,
		MaxTXID:          maxTXID,
		Timestamp:        maxTxn.Timestamp.UnixMilli(),
		PreApplyChecksum: minTxn.PreApplyChecksum,
	}); err != nil {
		return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("encode ltx header: %w", err)
	}

	// Iterate over each latest page between TXID range and write to encoder.
	rows, err := tx.QueryContext(ctx, `
		SELECT pgno, data
		FROM pages
		WHERE db_id = ?
		  AND min_txid >= ?
		  AND max_txid <= ?
		  AND pgno <= ?
		ORDER BY pgno ASC, max_txid DESC
	`,
		db.ID, minTxn.MinTXID, maxTxn.MaxTXID, maxTxn.Commit,
	)
	if err != nil {
		return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("query pages: %w", err)
	}

	pageData := make([]byte, db.PageSize)
	var prevPgno uint32
	for rows.Next() {
		var pgno uint32
		if err := rows.Scan(&pgno, &pageData); err != nil {
			return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("scan page: %w", err)
		}

		// Ensure we are only selecting the first instance of each page since
		// we are sorting by MaxTXID. This will give us the latest version.
		if pgno == prevPgno {
			continue
		}
		prevPgno = pgno

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageData); err != nil {
			return ltx.Header{}, ltx.Trailer{}, fmt.Errorf("encode ltx page %d: %w", pgno, err)
		}
	}
	if err := rows.Err(); err != nil {
		return ltx.Header{}, ltx.Trailer{}, err
	}

	enc.SetPostApplyChecksum(maxTxn.PostApplyChecksum)
	if err := enc.Close(); err != nil {
		return ltx.Header{}, ltx.Trailer{}, err
	}

	return enc.Header(), enc.Trailer(), nil
}

func encodeSnapshotPagesTo(ctx context.Context, tx *sql.Tx, dbID int, txID ltx.TXID, enc *ltx.Encoder) (ltx.Checksum, error) {
	stmt, err := tx.PrepareContext(ctx, `
		SELECT chksum, data
		FROM pages
		WHERE db_id = ? AND pgno = ? AND max_txid <= ?
		ORDER BY max_txid DESC
		LIMIT 1
	`)
	if err != nil {
		return 0, fmt.Errorf("prepare page query: %w", err)
	}

	hdr := enc.Header()
	data := make([]byte, hdr.PageSize)
	var postApplyChecksum ltx.Checksum
	for pgno := uint32(1); pgno <= hdr.Commit; pgno++ {
		if pgno == ltx.LockPgno(hdr.PageSize) {
			continue
		}

		var pageChksum ltx.Checksum
		if err := stmt.QueryRowContext(ctx, dbID, pgno, txID).Scan((*sqliteutil.Checksum)(&pageChksum), &data); err != nil {
			return 0, fmt.Errorf("read page %d: %w", pgno, err)
		}

		calcPageChksum := ltx.ChecksumPage(pgno, data)
		if pageChksum != calcPageChksum {
			return 0, fmt.Errorf("stored page checksum does not match calculated: pgno=%d stored=%016x calculated=%016x", pgno, pageChksum, calcPageChksum)
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, data); err != nil {
			return 0, fmt.Errorf("encode page %d: %w", pgno, err)
		}

		postApplyChecksum = ltx.ChecksumFlag | (postApplyChecksum ^ pageChksum)
	}

	return ltx.ChecksumFlag | postApplyChecksum, nil
}

// compactPagesBefore removes any duplicate pages before the given TXID by only
// retaining the latest version.
func compactPagesBefore(ctx context.Context, tx *sql.Tx, dbID int, txID ltx.TXID) error {
	// We want to compact away any pages at the given TXID or earlier so we
	// actually want to query for the next highest minimum TXID.
	rows, err := tx.QueryContext(ctx, `
		SELECT pgno, MAX(max_txid)
		FROM pages
		WHERE db_id = ? AND min_txid <= ?
		GROUP BY pgno
		HAVING COUNT(*) > 1
	`,
		dbID, txID,
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var pgno uint32
		var maxTXID ltx.TXID
		if err := rows.Scan(&pgno, &maxTXID); err != nil {
			return err
		}
		if maxTXID > txID {
			return fmt.Errorf("unexpected, maxTXID %s is greater than txID %s", maxTXID, txID)
		}

		if _, err := tx.ExecContext(ctx, `
			DELETE FROM pages
			WHERE db_id = ?
			AND pgno = ?
			AND max_txid < ?
		`, dbID, pgno, maxTXID); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return rows.Close()
}

func insertPages(ctx context.Context, tx *sql.Tx, db *DB, txn *Txn, req *WriteTxRequest) (ltx.Checksum, error) {
	prevPageStmt, err := tx.PrepareContext(ctx, `
		SELECT data
		FROM pages
		WHERE db_id = ? AND pgno = ? AND max_txid <= ?
		ORDER BY max_txid DESC
		LIMIT 1
	`)
	if err != nil {
		return 0, fmt.Errorf("prepare page query: %w", err)
	}

	insertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO pages (db_id, pgno, min_txid, max_txid, chksum, data)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, err
	}

	// Start with the previous checksum and verify that the resulting checksum
	// is correct. If this a multi-batch write then we continue with the partially
	// updated checksum that was stored in the Txn from the last batch. This way
	// we can do small incremental calculations rather than one big one at the end.
	postApplyChecksum := req.PreApplyChecksum
	if req.WriteIndex > 0 {
		postApplyChecksum = txn.PostApplyChecksum
	}

	prevPageData := make([]byte, req.PageSize)
	for _, page := range req.Pages {
		// Skip if previous version of the page is the same.
		if page.Pgno <= db.Commit {
			if err := prevPageStmt.QueryRowContext(ctx, db.ID, page.Pgno, db.TXID).Scan(&prevPageData); err == sql.ErrNoRows {
				return 0, lfsb.Errorf(lfsb.ErrorTypeConflict, "ENOPREVPAGE", "prev page %d not found @ %s", page.Pgno, db.TXID)
			} else if err != nil {
				return 0, fmt.Errorf("read prev page %d @ %s: %w", page.Pgno, db.TXID, err)
			}

			// If the new page and previous page have the same data then we
			// can discard the new page.
			if bytes.Equal(page.Data, prevPageData) {
				continue
			}

			// Remove old page checksum.
			prevChksum := ltx.ChecksumPage(page.Pgno, prevPageData)
			postApplyChecksum = ltx.ChecksumFlag | (postApplyChecksum ^ prevChksum)
		}

		// Add new page checksum.
		chksum := ltx.ChecksumPage(page.Pgno, page.Data)
		postApplyChecksum = ltx.ChecksumFlag | (postApplyChecksum ^ chksum)

		if _, err := insertStmt.ExecContext(ctx,
			db.ID,
			page.Pgno,
			req.MinTXID,
			req.MaxTXID,
			sqliteutil.Checksum(chksum),
			page.Data,
		); err != nil {
			return 0, fmt.Errorf("insert page %d: %w", page.Pgno, err)
		}
	}

	// On the last batch, truncate page checksum from shrinking the database
	// and perform some validation before we finally commit.
	if req.IsLastBatch() {
		// Remove checksums from truncated pages.
		lockPgno := ltx.LockPgno(req.PageSize)
		for pgno := req.Commit + 1; pgno <= db.Commit; pgno++ {
			if pgno == lockPgno {
				continue
			}

			if err := prevPageStmt.QueryRowContext(ctx, db.ID, pgno, db.TXID).Scan(&prevPageData); err != nil {
				return 0, fmt.Errorf("read truncated page %d @ %s: %w", pgno, db.TXID, err)
			}

			prevChksum := ltx.ChecksumPage(pgno, prevPageData)
			postApplyChecksum = ltx.ChecksumFlag | (postApplyChecksum ^ prevChksum)
		}

		// Ensure all additional pages exist when we extend the database size.
		if req.Commit > db.Commit {
			var n int
			if err := tx.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM pages
				WHERE db_id = ?
				  AND pgno > ?
				  AND min_txid = ?
				  AND max_txid = ?
			`,
				db.ID, db.Commit, req.MinTXID, req.MaxTXID,
			).Scan(
				&n,
			); err != nil {
				return 0, fmt.Errorf("count extended pages: %w", err)
			}

			// Determine the expected number of pages. If we cross the lock page
			// boundary then we should remove that page as we don't track it.
			diff := int(req.Commit - db.Commit)
			if db.Commit < lockPgno && req.Commit >= lockPgno {
				diff--
			}

			if diff != n {
				return 0, lfsb.Errorf(lfsb.ErrorTypeValidation, "EMISSINGPAGES", "database extend without all pages, expected %d new pages, found %d pages", diff, n)
			}
		}
	}

	return postApplyChecksum, nil
}
