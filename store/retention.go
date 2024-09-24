package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/db/sqliteutil"
	"github.com/superfly/ltx"
)

func (s *Store) DeleteCluster(ctx context.Context, cluster string) error {
	dbs, err := s.FindDBsByCluster(ctx, cluster)
	if err != nil {
		return fmt.Errorf("find dbs: %w", err)
	}

	slog.Info("deleting cluster",
		slog.String("cluster", cluster),
		slog.Int("dbs", len(dbs)))

	for _, db := range dbs {
		if err := s.DropDB(ctx, cluster, db.Name); err != nil {
			return fmt.Errorf("drop db %s/%s: %w", cluster, db.Name, err)
		}
	}

	return nil
}

// DropDB hard deletes the database from the shard database and remote storage.
func (s *Store) DropDB(ctx context.Context, cluster, database string) error {
	for level := 1; level <= lfsb.CompactionLevelSnapshot; level++ {
		paths, err := FindStoragePaths(ctx, s.RemoteClient, cluster, database, level, nil)
		if err != nil {
			return err
		} else if len(paths) == 0 {
			continue
		}

		slog.Info("deleting database remote storage files",
			slog.String("cluster", cluster),
			slog.String("db", database),
			slog.Int("level", level),
			slog.Int("n", len(paths)))

		if err := s.RemoteClient.DeleteFiles(ctx, paths); err != nil {
			return err
		}
	}

	tx, err := sqliteutil.BeginImmediate(s.db)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	db, err := findDBByName(ctx, tx, cluster, database)
	if err != nil {
		return err
	}

	if err := deleteDB(ctx, tx, db.ID); err != nil {
		return err
	}

	return tx.Commit()
}

// EnforceRemoteRetention removes all files before a given TXID that are past the retention period.
func (s *Store) EnforceRemoteRetention(ctx context.Context, cluster, database string, level int, maxTXID ltx.TXID) error {
	t := time.Now()

	lvl := s.Levels[level]
	minTime := t.Add(-lvl.Retention)

	logger := s.dbLogger(cluster, database).With(slog.Int("level", level))

	allPaths, err := FindStoragePaths(ctx, s.RemoteClient, cluster, database, level, func(p StoragePath) (bool, error) {
		return p.MinTXID < maxTXID, nil
	})
	if err != nil {
		return fmt.Errorf("find storage paths: %w", err)
	}

	// Filter out paths that are older than the retention period. We perform
	// the filter separately than above so we can stop filtering when we see a
	// retained file. This avoids gaps in the storage files.
	deletedPaths := make([]StoragePath, 0, len(allPaths))
	for _, path := range allPaths {
		// If a retention duration is set and the file is within the retention
		// period then we'll keep the file and all files after it.
		if lvl.Retention != 0 && path.CreatedAt.After(minTime) {
			break
		}
		deletedPaths = append(deletedPaths, path)
	}

	// Skip if we don't have any matching paths.
	if len(deletedPaths) == 0 {
		return nil
	}

	if err := s.RemoteClient.DeleteFiles(ctx, deletedPaths); err != nil {
		return fmt.Errorf("delete files: %w", err)
	}

	logger.Debug("removed files per retention",
		slog.Int("n", len(deletedPaths)),
		slog.Group("txid",
			slog.String("min", deletedPaths[0].MinTXID.String()),
			slog.String("max", deletedPaths[len(deletedPaths)-1].MaxTXID.String()),
		),
		slog.Duration("elapsed", time.Since(t)),
	)

	return nil
}
