package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/superfly/ltx"
)

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
