package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/db/sqliteutil"
	"github.com/superfly/ltx"
)

const (
	MaxCompactionFileN = 100
)

// CompactDBToLevel compacts all transaction files from the next lower level since
// the last TXID in the level to be compacted. This ensures that higher level
// compactions line up on TXID with lower levels and makes it easier to compute
// the list of files needed to perform a restore.
//
// Returns the path of the newly compacted storage path.
// Returns ENOCOMPACTION if no compaction occurred.
// Returns EPARTIALCOMPACTION if there were too many files to compact.
func (s *Store) CompactDBToLevel(ctx context.Context, cluster, database string, dstLevel int) (StoragePath, error) {
	// Verify that we are compacting to a level that exists and is not L0.
	// Level 0 is the raw LTX files that we receive from the client.
	if dstLevel == 0 {
		return StoragePath{}, lfsb.ErrCannotCompactToLevelZero
	} else if !s.Levels.IsValidLevel(dstLevel) {
		return StoragePath{}, lfsb.ErrCompactionLevelTooHigh
	}

	switch dstLevel {
	case 1:
		return s.compactDBToL1(ctx, cluster, database)
	case lfsb.CompactionLevelSnapshot:
		return s.compactDBToSnapshot(ctx, cluster, database)
	default:
		return s.compactDBToLevel(ctx, cluster, database, dstLevel)
	}
}

// compactDBToL1 compacts from the L0 shard database into L1 in remote storage.
func (s *Store) compactDBToL1(ctx context.Context, cluster, database string) (StoragePath, error) {
	logger := s.dbLogger(cluster, database).With(
		slog.Group("level", slog.Int("src", 0), slog.Int("dst", 1)))

	// Compact to a temporary file so we can extract metadata.
	tempFile, err := s.createTemp("l0-*.ltx")
	if err != nil {
		return StoragePath{}, err
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()
	defer func() { _ = tempFile.Close() }()

	// Determine the starting TXID based on the highest TXID in the level.
	_, prevMaxTXID, err := StorageTXIDRange(ctx, s.RemoteClient, cluster, database, 1)
	if err != nil {
		return StoragePath{}, fmt.Errorf("level max transaction id: %w", err)
	}
	minTXID := prevMaxTXID + 1

	header, trailer, err := s.WriteLTXPageRangeFrom(ctx, cluster, database, minTXID, tempFile)
	if err != nil {
		return StoragePath{}, err
	}
	maxTXID := header.MaxTXID

	logger.Debug("compacting to level", slog.String("min_txid", minTXID.String()))

	// Output file is the range of the lowest TXID to the highest TXID.
	outPath := StoragePath{
		Cluster:  cluster,
		Database: database,
		Level:    1,
		MinTXID:  minTXID,
		MaxTXID:  maxTXID,
		Metadata: NewStorageMetadataFromLTX(header, trailer),
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return StoragePath{}, err
	} else if err := s.RemoteClient.WriteFile(ctx, outPath, tempFile); err != nil {
		return StoragePath{}, fmt.Errorf("write storage file: %w", err)
	}

	startTime := time.Now()

	// Apply L0 compaction in sqlite db.
	now := s.Now()
	retention := s.Levels[0].Retention

	tx, err := sqliteutil.BeginImmediate(s.db)
	if err != nil {
		return StoragePath{}, err
	}
	defer tx.Rollback()

	db, err := findDBByName(ctx, tx, cluster, database)
	if err != nil {
		return StoragePath{}, err
	}

	// during compaction, we remove old stuff that's beyond the retention window
	if retention > 0 {
		txn, err := findMaxTxnBeforeTimestamp(ctx, tx, db.ID, now.Add(-retention))
		if err != nil && errors.Is(err, lfsb.ErrTxNotAvailable) {
			return StoragePath{}, fmt.Errorf("find max txn before timestamp: %w", err)
		} else if txn != nil && txn.MaxTXID <= maxTXID {
			if err := deleteTxnsBeforeMaxTXID(ctx, tx, db.ID, maxTXID); err != nil {
				return StoragePath{}, fmt.Errorf("delete txns before %s: %w", maxTXID, err)
			}
			if err := compactPagesBefore(ctx, tx, db.ID, maxTXID); err != nil {
				return StoragePath{}, fmt.Errorf("compact pages before %s: %w", maxTXID, err)
			}
		}
	}

	// L0 compaction occurs because pages are being written to L1 which, in turn,
	// means that we need to trigger a compaction for the next level (L2).
	if err := requestCompaction(ctx, tx, db.ID, 2, rand.Int()); err != nil {
		return StoragePath{}, fmt.Errorf("request next level compaction: %w", err)
	}

	if err := increaseHWM(ctx, tx, db.ID, maxTXID); err != nil {
		return StoragePath{}, fmt.Errorf("increase hwm: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return StoragePath{}, err
	}

	logger.Debug("L0 pages compacted", slog.Duration("elapsed", time.Since(startTime)))

	return outPath, nil
}

// compactDBToLevel handles any compactions sourced from L1 or higher.
func (s *Store) compactDBToLevel(ctx context.Context, cluster, database string, dstLevel int) (StoragePath, error) {
	if dstLevel < 2 {
		return StoragePath{}, fmt.Errorf("invalid destination level")
	}

	srcLevel := s.Levels.PrevLevel(dstLevel)

	logger := s.dbLogger(cluster, database).With(
		slog.Group("level",
			slog.Int("src", srcLevel),
			slog.Int("dst", dstLevel),
		),
	)

	// Determine the starting TXID based on the highest TXID in the level.
	_, prevMaxTXID, err := StorageTXIDRange(ctx, s.RemoteClient, cluster, database, dstLevel)
	if err != nil {
		return StoragePath{}, fmt.Errorf("level max transaction id: %w", err)
	}
	minTXID := prevMaxTXID + 1

	// Iterate over list of LTX files in the source level after TXID.
	paths, err := FindStoragePaths(ctx, s.RemoteClient, cluster, database, srcLevel, func(p StoragePath) (bool, error) {
		return p.MinTXID >= minTXID, nil
	})
	if err != nil {
		return StoragePath{}, fmt.Errorf("find storage paths: %w", err)
	} else if len(paths) == 0 {
		logger.Debug("no new data, skipping compaction")
		return StoragePath{}, lfsb.Errorf(lfsb.ErrorTypeNotFound, lfsb.ENOCOMPACTION, "no compaction") // no new data, skip compaction
	}

	// Limit the number of paths that we compact at once.
	var isPartial bool
	if n := MaxCompactionFileN; len(paths) > n {
		logger.Debug("too many paths in one compaction, performing a partial compaction", slog.Int("paths", len(paths)))
		isPartial = true
		paths = paths[:n]
	}

	logger.Debug("compacting to level",
		slog.String("min_txid", minTXID.String()),
		slog.Int("paths", len(paths)),
	)

	// Create list of readers for the compactor; ensure they close by function exit.
	rdrs := make([]io.Reader, 0, len(paths))
	defer func() {
		for _, r := range rdrs {
			_ = r.(io.Closer).Close()
		}
	}()

	// Open files from long-term storage. Compactions should be in the tens of
	// sources so we shouldn't need to worry about an excessively large number
	// of files.
	for i := range paths {
		f, err := s.RemoteClient.OpenFile(ctx, paths[i])
		if err != nil {
			return StoragePath{}, fmt.Errorf("open storage file: %w", err)
		}
		rdrs = append(rdrs, f)
	}

	// Output file is the range of the lowest TXID to the highest TXID.
	outPath := StoragePath{
		Cluster:  cluster,
		Database: database,
		Level:    dstLevel,
		MinTXID:  paths[0].MinTXID,
		MaxTXID:  paths[len(paths)-1].MaxTXID,
	}

	t := time.Now()

	// Compact to a temporary file so we can extract metadata.
	tempFile, err := s.createTemp(fmt.Sprintf("l%d-*.ltx", dstLevel))
	if err != nil {
		return StoragePath{}, err
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()
	defer func() { _ = tempFile.Close() }()

	c := ltx.NewCompactor(tempFile, rdrs)
	c.HeaderFlags = ltx.HeaderFlagCompressLZ4
	if err := c.Compact(ctx); err != nil {
		return StoragePath{}, fmt.Errorf("compact ltx file: %w", err)
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return StoragePath{}, err
	}

	logger.Debug("level written",
		slog.Group("txid",
			slog.String("min", outPath.MinTXID.String()),
			slog.String("max", outPath.MaxTXID.String()),
		),
		slog.Duration("elapsed", time.Since(t)),
	)

	t = time.Now()

	// Attach metadata before writing to storage.
	outPath.Metadata = NewStorageMetadataFromLTX(c.Header(), c.Trailer())
	if err := s.RemoteClient.WriteFile(ctx, outPath, tempFile); err != nil {
		return StoragePath{}, fmt.Errorf("write storage file: %w", err)
	}

	// Delete all files that have been compacted and are older than our retention period.
	// if err := s.EnforceRemoteRetention(ctx, cluster, database, srcLevel, paths[len(paths)-1].MinTXID); err != nil {
	// 	return StoragePath{}, fmt.Errorf("enforce retention (L0): %w", err)
	// }

	logger.Debug("level uploaded",
		slog.Group("txid",
			slog.String("min", outPath.MinTXID.String()),
			slog.String("max", outPath.MaxTXID.String()),
		),
		slog.Duration("elapsed", time.Since(t)),
	)

	// Mark next level as requiring compaction.
	nextLevel := s.Levels.NextLevel(dstLevel)
	if nextLevel != -1 {
		tx, err := sqliteutil.BeginImmediate(s.db)
		if err != nil {
			return StoragePath{}, err
		}
		defer tx.Rollback()

		db, err := findDBByName(ctx, tx, cluster, database)
		if err != nil {
			return StoragePath{}, err
		}

		if err := requestCompaction(ctx, tx, db.ID, nextLevel, rand.Int()); err != nil {
			return StoragePath{}, fmt.Errorf("request compaction: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return StoragePath{}, err
		}
	}

	// If this is a partial compaction, return the appropriate error so we
	// will retry again on the next attempt.
	if isPartial {
		return StoragePath{}, lfsb.Errorf(lfsb.ErrorTypeUnprocessable, lfsb.EPARTIALCOMPACTION, "partial compaction")
	}

	return outPath, nil
}

// compactDBToSnapshot generates a snapshot LTX file which goes from TXID 1 to
// the current TXID. Returns the path of the newly compacted storage path.
// Returns ENOCOMPACTION if no compaction occurred.
func (s *Store) compactDBToSnapshot(ctx context.Context, cluster, database string) (StoragePath, error) {
	logger := s.dbLogger(cluster, database)

	// Determine max TXID of the highest non-snapshot level.
	maxPath, err := MaxStoragePath(ctx, s.RemoteClient, cluster, database, s.Levels.MaxLevel())
	if maxPath.IsZero() {
		logger.Debug("no data in highest non-snapshot level, skipping compaction")
		return StoragePath{}, lfsb.Errorf(lfsb.ErrorTypeNotFound, lfsb.ENOCOMPACTION, "no compaction") // no new data, skip compaction
	} else if err != nil {
		return StoragePath{}, fmt.Errorf("find max path from highest non-snapshot level: %w", err)
	}
	maxTXID := maxPath.MaxTXID

	// Ensure new data has been written to highest level since last snapshot.
	paths, err := FindStoragePaths(ctx, s.RemoteClient, cluster, database, lfsb.CompactionLevelSnapshot, nil)
	if err != nil {
		return StoragePath{}, fmt.Errorf("find snapshot storage paths: %w", err)
	} else if len(paths) > 0 && paths[len(paths)-1].MaxTXID == maxPath.MaxTXID {
		logger.Debug("no new data, skipping snaphot", slog.String("txid", maxTXID.String()))
		return StoragePath{}, lfsb.Errorf(lfsb.ErrorTypeNotFound, lfsb.ENOCOMPACTION, "no compaction") // no new data, skip compaction
	}

	logger.Debug("compacting to snapshot",
		slog.Group("txid",
			slog.String("min", ltx.TXID(1).String()),
			slog.String("max", maxTXID.String()),
		),
	)

	t := time.Now()

	// Output file is the range of the lowest TXID to the highest TXID.
	outPath := StoragePath{
		Cluster:  cluster,
		Database: database,
		Level:    lfsb.CompactionLevelSnapshot,
		MinTXID:  1,
		MaxTXID:  maxTXID,
	}

	// Compact to a temporary file so we can extract metadata.
	tempFile, err := s.createTemp("snapshot-*.ltx")
	if err != nil {
		return StoragePath{}, err
	}
	defer func() { _ = os.Remove(tempFile.Name()) }()
	defer func() { _ = tempFile.Close() }()

	// Retrieve a list of input LTX files.
	inputPaths, err := CalcSnapshotPlan(ctx, s.RemoteClient, cluster, database, maxTXID)
	if err != nil {
		return StoragePath{}, fmt.Errorf("calc snapshot plan: %w", err)
	}

	c, close, err := NewCompactorFromPaths(ctx, s.RemoteClient, inputPaths, tempFile)
	if err != nil {
		return StoragePath{}, fmt.Errorf("snapshot compactor: %w", err)
	}
	defer func() { _ = close() }()

	if err := c.Compact(ctx); err != nil {
		return StoragePath{}, fmt.Errorf("compact ltx file: %w", err)
	}
	if err := close(); err != nil {
		return StoragePath{}, fmt.Errorf("close compaction input files: %w", err)
	}

	logger.Debug("snapshot written",
		slog.Group("txid",
			slog.String("min", outPath.MinTXID.String()),
			slog.String("max", outPath.MaxTXID.String()),
		),
		slog.Duration("elapsed", time.Since(t)),
	)

	t = time.Now()

	// Attach metadata before writing to storage.
	outPath.Metadata = NewStorageMetadataFromLTX(c.Header(), c.Trailer())
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return StoragePath{}, err
	}
	if err := s.RemoteClient.WriteFile(ctx, outPath, tempFile); err != nil {
		return StoragePath{}, fmt.Errorf("write storage file: %w", err)
	}

	logger.Debug("snapshot compacted",
		slog.Group("txid",
			slog.String("min", outPath.MinTXID.String()),
			slog.String("max", outPath.MaxTXID.String()),
		),
		slog.Duration("elapsed", time.Since(t)),
	)

	return outPath, nil
}