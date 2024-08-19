package sqliteutil

import (
	"database/sql"

	"github.com/mattn/go-sqlite3"
)

func init() {
	sql.Register("lfsb-sqlite",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				_, err := conn.Exec(`
					PRAGMA busy_timeout       = 30000;
					PRAGMA journal_mode       = WAL;
					PRAGMA journal_size_limit = 4194304;
					PRAGMA synchronous        = FULL;
					PRAGMA foreign_keys       = ON;
					PRAGMA cache_size         = -2000;
				`, nil)

				return err
			},
		},
	)
}
