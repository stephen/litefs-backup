package store

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/db/sqliteutil"
	"github.com/superfly/ltx"
)

const (
	DefaultWriteTxBatchSize    = 4 << 20 // 4MB
	DefaultWriteTxBatchTimeout = 5 * time.Second
)

type WriteTxOptions struct {
	AppendSnapshot bool
	Timestamp      time.Time
	// LeaseID        string // XXX: should be able to delete; for litefs vfs.
}

type WriteTxRequest struct {
	Cluster           string
	Database          string
	PageSize          uint32
	Commit            uint32
	MinTXID           ltx.TXID
	MaxTXID           ltx.TXID
	Timestamp         time.Time
	PreApplyChecksum  ltx.Checksum
	PostApplyChecksum ltx.Checksum
	WriteKey          int64
	WriteIndex        int64
	WriteExpiresAt    time.Time
	AppendSnapshot    bool
	LeaseID           string
	IdempotencyKey    int64
	Now               time.Time
	Pages             []*WriteTxPage
}

// IsLastBatch returns true if the post-apply checksum has been set.
// This indicates that the LTX trailer was available and the write is complete.
func (req *WriteTxRequest) IsLastBatch() bool {
	return req.PostApplyChecksum != 0
}

type WriteTxPage struct {
	Pgno uint32
	Data []byte
}

func NewStore() *Store {
	return &Store{
		WriteTxBatchSize:    DefaultWriteTxBatchSize,
		WriteTxBatchTimeout: DefaultWriteTxBatchTimeout,
	}
}

type Store struct {
	db *sql.DB

	WriteTxBatchSize    int64
	WriteTxBatchTimeout time.Duration
}

// WriteTx appends an LTX file to a database.
func (s *Store) WriteTx(ctx context.Context, cluster, database string, r io.Reader, opt *WriteTxOptions) (ltx.Pos, error) {
	if opt == nil {
		opt = &WriteTxOptions{}
	}

	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		return ltx.Pos{}, fmt.Errorf("decode header: %w", err)
	}
	hdr := dec.Header()

	// XXX what is this
	if opt.AppendSnapshot && !hdr.IsSnapshot() {
		return ltx.Pos{}, fmt.Errorf("ltx file must be a snapshot to append")
	}

	// XXX what is this
	// Override the header timestamp, if requested.
	timestamp := time.UnixMilli(hdr.Timestamp).UTC()
	if !opt.Timestamp.IsZero() {
		timestamp = opt.Timestamp.UTC()
	}

	// TODO(raft): Propose cancelation message if we don't reach the end of the LTX file.

	// Generate request once so we can reuse it for each batch.
	req := &WriteTxRequest{
		Cluster:          cluster,
		Database:         database,
		WriteKey:         rand.Int63(),
		PageSize:         hdr.PageSize,
		Commit:           hdr.Commit,
		MinTXID:          hdr.MinTXID,
		MaxTXID:          hdr.MaxTXID,
		Timestamp:        timestamp,
		PreApplyChecksum: hdr.PreApplyChecksum,
		AppendSnapshot:   opt.AppendSnapshot,
		IdempotencyKey:   int64(rand.Int()),
	}

	for i := 0; ; i++ {
		req.WriteIndex = int64(i)

		if pos, err := s.writeTxBatch(ctx, dec, req); err != nil {
			return ltx.Pos{}, fmt.Errorf("write batch %d: %w", i, err)
		} else if !pos.IsZero() {
			return pos, nil
		}
	}
}

func (s *Store) writeTxBatch(ctx context.Context, dec *ltx.Decoder, req *WriteTxRequest) (ltx.Pos, error) {
	maxPageN := int(s.WriteTxBatchSize / int64(req.PageSize))
	req.Pages = nil

	// Read next chunk of pages from decoder.
	var eof bool
	for i := 0; i < maxPageN; i++ {
		var hdr ltx.PageHeader
		data := make([]byte, req.PageSize)
		if err := dec.DecodePage(&hdr, data); err == io.EOF {
			eof = true
			break
		} else if err != nil {
			return ltx.Pos{}, fmt.Errorf("decode page: %w", err)
		}

		req.Pages = append(req.Pages, &WriteTxPage{
			Pgno: hdr.Pgno,
			Data: data,
		})
	}

	// Read trailer once we've finished with pages. We'll attach the post-apply
	// checksum to indicate that this is the final write of the batch.
	if eof {
		if err := dec.Close(); err != nil {
			return ltx.Pos{}, fmt.Errorf("close ltx decoder: %w", err)
		}
		req.PostApplyChecksum = dec.Trailer().PostApplyChecksum
	}

	// Send current time & a write timeout so that the Raft message is deterministic.
	now := time.Now().UTC()
	req.WriteExpiresAt = now.Add(s.WriteTxBatchTimeout)
	req.Now = now

	tx, err := sqliteutil.BeginImmediate(s.db)
	if err != nil {
		return ltx.Pos{}, err
	}
	defer tx.Rollback()

	pos, err := s.applyWriteTx(ctx, tx, req)
	if err != nil {
		return ltx.Pos{}, err
	}

	if err := tx.Commit(); err != nil {
		return ltx.Pos{}, err
	}

	return pos, nil
}

func (s *Store) dbLogger(cluster, database string) *slog.Logger {
	return slog.With(
		slog.String("cluster", cluster),
		slog.String("database", database),
	)
}

func (s *Store) applyWriteTx(ctx context.Context, tx *sql.Tx, req *WriteTxRequest) (ltx.Pos, error) {
	logger := s.dbLogger(req.Cluster, req.Database)

	if req.WriteKey == 0 {
		return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, "EINVALIDWRITEKEY", "txn write key required")
	}

	if req.Timestamp.IsZero() {
		return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, "EINVALIDTIMESTAMP", "txn timestamp must be set")
	} else if req.WriteExpiresAt.IsZero() {
		return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, "EINVALIDWRITEEXPIRATION", "txn write expiration must be set")
	} else if req.Now.IsZero() {
		return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, "EINVALIDNOW", "txn current time must be set")
	}
	ltxTimestamp := req.Timestamp
	writeExpiresAt := req.WriteExpiresAt
	now := req.Now

	// Lookup the database entry so we have a database ID to work with.
	db := &DB{
		Cluster: req.Cluster,
		Name:    req.Database,
	}
	if err := createDBIfNotExist(ctx, tx, db); err != nil {
		return ltx.Pos{}, fmt.Errorf("create database: %w", err)
	}

	if db.PageSize == 0 {
		db.PageSize = req.PageSize
	} else if db.PageSize != req.PageSize {
		return ltx.Pos{}, lfsb.ErrPageSizeMismatch
	}

	// If this is a restore or a replacement, update the header to be contiguous
	// with the previous transaction.
	if req.AppendSnapshot {
		req.MinTXID = db.TXID + 1
		req.MaxTXID = db.TXID + 1
		req.PreApplyChecksum = db.PostApplyChecksum
	}

	// Verify that LTX file is contiguous.
	preApplyPos := ltx.Pos{
		TXID:              ltx.TXID(req.MinTXID - 1),
		PostApplyChecksum: req.PreApplyChecksum,
	}
	if db.Pos() != preApplyPos {
		// If we've already started writing a transaction and then exceeded the
		// timeout then another multibatch transaction can be inserted in between
		// The position will be mismatched but we actually want to show as a timeout.
		if req.WriteIndex > 0 {
			return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeConflict, "ETXTIMEOUT", "write transaction timed out")
		}

		logger.Debug("tx position mismatch",
			slog.String("pos", db.Pos().String()),
			slog.Group("ltx",
				slog.String("min_txid", ltx.TXID(req.MinTXID).String()),
				slog.String("pre_apply_checksum", fmt.Sprintf("%x016x", req.PreApplyChecksum)),
			),
		)
		return ltx.Pos{}, ltx.NewPosMismatchError(db.Pos())
	}

	// Lookup existing transaction with the same TXID range.
	txn, err := findPendingTxnByMinTXID(ctx, tx, db.ID, ltx.TXID(req.MinTXID))
	if err != nil && err != lfsb.ErrTxNotAvailable {
		return ltx.Pos{}, fmt.Errorf("find existing txn: %w", err)
	}

	// If a different transaction exists but is expired then remove it.
	if txn != nil && txn.WriteKey != req.WriteKey && txn.WriteExpiresAt.Before(now) {
		if err := deletePendingTxnAndPages(ctx, tx, db.ID, txn.MinTXID, txn.MaxTXID); err != nil {
			return ltx.Pos{}, fmt.Errorf("expire pending txn: %w", err)
		}
		txn = nil
	}

	// If no transaction exists, start a new one.
	if txn == nil {
		if req.WriteIndex != 0 {
			return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, "EINVALIDWRITEINDEX", "txn write index must start at zero")
		}

		txn = &Txn{
			DBID:              db.ID,
			MinTXID:           ltx.TXID(req.MinTXID),
			MaxTXID:           ltx.TXID(req.MaxTXID),
			PageSize:          req.PageSize,
			Commit:            req.Commit,
			Timestamp:         ltxTimestamp,
			PreApplyChecksum:  req.PreApplyChecksum,
			PostApplyChecksum: ltx.ChecksumFlag, // temporary, updated before end of tx
			WriteKey:          req.WriteKey,
			WriteIndex:        req.WriteIndex,
			WriteExpiresAt:    &writeExpiresAt,
		}

		if err := createTxn(ctx, tx, txn); err != nil {
			return ltx.Pos{}, fmt.Errorf("create txn: %w", err)
		}
	} else {
		// If there is an in-progress transaction then continue it if the
		// write key matches and the write index is contiguous. Expired txns
		// are handled above so we don't need to check for those.
		if txn.WriteKey != req.WriteKey {
			return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeConflict, "ETXPENDING", "cannot write while transaction in progress")
		} else if txn.WriteIndex+1 != req.WriteIndex {
			return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeConflict, "ETXBADSEQ", "write batch out of order (%d,%d)", txn.WriteIndex, req.WriteIndex)
		}
	}

	// Inserting pages will return a rolling post apply checksum. If this is a
	// multibatch write then we'll save the checksum to the Txn so we can
	// continue computing it on the next batch.
	postApplyChecksum, err := insertPages(ctx, tx, db, txn, req)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("insert pages: %w", err)
	}

	// Verify resulting checksum matches the header's post-apply checksum.
	if req.IsLastBatch() {
		if req.PostApplyChecksum != postApplyChecksum {
			return ltx.Pos{}, lfsb.Errorf(lfsb.ErrorTypeValidation, "EBADCHECKSUM", "incorrect post-apply checksum of %016x, expected %016x",
				req.PostApplyChecksum, postApplyChecksum)
		}
	}

	// Update write fields and the rolling checksum.
	txn.PostApplyChecksum = postApplyChecksum
	if req.IsLastBatch() {
		txn.WriteKey, txn.WriteIndex = 0, 0
		txn.WriteExpiresAt = nil
	} else {
		txn.WriteKey, txn.WriteIndex = req.WriteKey, req.WriteIndex
		t := req.WriteExpiresAt
		txn.WriteExpiresAt = &t
	}

	if _, err := tx.Exec(`
		UPDATE txns
		SET post_apply_checksum = ?,
		    write_key = ?,
		    write_index = ?,
		    write_expires_at = ?
		WHERE db_id = ? AND min_txid = ? AND max_txid = ?
	`,
		sqliteutil.Checksum(postApplyChecksum),
		txn.WriteKey,
		txn.WriteIndex,
		sqliteutil.NewNullTime(txn.WriteExpiresAt),
		db.ID,
		req.MinTXID,
		req.MaxTXID,
	); err != nil {
		return ltx.Pos{}, fmt.Errorf("finalize txn state: %w", err)
	}

	// If this is not the last batch, exit early.
	if !req.IsLastBatch() {
		logger.Debug("tx batch written", slog.Int64("index", req.WriteIndex))
		return ltx.Pos{}, nil
	}

	// Request L1 compaction.
	if err := requestCompaction(ctx, tx, db.ID, 1, int(req.IdempotencyKey)); err != nil {
		return ltx.Pos{}, fmt.Errorf("request compaction: %w", err)
	}

	if req.MinTXID == 1 {
		logger.Debug("database created", slog.Int64("size", int64(req.Commit)*int64(req.PageSize)))
	}

	logger.Debug("tx batch finalized",
		slog.Int64("n", req.WriteIndex),
		slog.String("pos", txn.PostApplyPos().String()))

	return ltx.Pos{
		TXID:              txn.MaxTXID,
		PostApplyChecksum: txn.PostApplyChecksum,
	}, nil
}

// FindDBByName returns a database entry by org/cluster/name.
func (s *Store) FindDBByName(ctx context.Context, cluster, name string) (*DB, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	return findDBByName(ctx, tx, cluster, name)
}