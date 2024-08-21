package store

import (
	"context"
)

// CompactionRequest represents a request for a database to be compacted at
// a given level in the future. It contains an idempotency key as it needs to
// avoid the race condition where a new compaction request occurs while the
// request is being processed.
type CompactionRequest struct {
	Cluster        string
	Database       string
	Level          int
	IdempotencyKey int
}

func findCompactionRequestsByLevel(ctx context.Context, tx DBTX, level int) ([]*CompactionRequest, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT d.cluster, d.name, level, idempotency_key
		FROM compaction_requests cr
		INNER JOIN databases d ON cr.db_id = d.id
		WHERE level = ?
		ORDER BY d.cluster, d.name
	`,
		level,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var a []*CompactionRequest
	for rows.Next() {
		var req CompactionRequest
		if err := rows.Scan(&req.Cluster, &req.Database, &req.Level, &req.IdempotencyKey); err != nil {
			return nil, err
		}
		a = append(a, &req)
	}

	if err := rows.Err(); err != nil {
		return a, err
	}
	return a, rows.Close()
}

func requestCompaction(ctx context.Context, tx DBTX, dbID int, level, idempotencyKey int) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO compaction_requests (db_id, level, idempotency_key) VALUES (?, ?, ?)
		ON CONFLICT (db_id, level)
		DO UPDATE SET idempotency_key = ?
	`,
		dbID, level, idempotencyKey, idempotencyKey,
	)
	return err
}

func markCompactionRequestComplete(ctx context.Context, tx DBTX, dbID, level, idempotencyKey int) error {
	_, err := tx.ExecContext(ctx, `
		DELETE FROM compaction_requests
		WHERE db_id = ? AND level = ? AND idempotency_key = ?
	`,
		dbID, level, idempotencyKey,
	)
	return err
}
