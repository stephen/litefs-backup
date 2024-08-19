package sqliteutil

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/superfly/ltx"
)

// BeginImmediate returns a transaction that has already acquired the write lock.
func BeginImmediate(db *sql.DB) (*sql.Tx, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}

	// HACK: This resets the transaction so that it starts with a write lock.
	// See: https://github.com/mattn/go-sqlite3/issues/400#issuecomment-598953685
	if _, err = tx.Exec("ROLLBACK; BEGIN IMMEDIATE"); err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	return tx, nil
}

// Time represents a SQLite-specific implementation of non-nullable time.Time.
type Time time.Time

// Scan reads an LTX timestamp into t.
func (t *Time) Scan(src any) error {
	if src == nil {
		*(*time.Time)(t) = time.Time{}
		return nil
	}

	switch src := src.(type) {
	case string:
		v, err := ltx.ParseTimestamp(src)
		if err != nil {
			return err
		}
		*(*time.Time)(t) = v.UTC()
		return nil
	default:
		return fmt.Errorf("invalid time format: %T", src)
	}
}

// Value returns the time in LTX timestamp format.
func (t Time) Value() (driver.Value, error) {
	return ltx.FormatTimestamp((time.Time)(t)), nil
}

// NullTime represents a SQLite-specific implementation of nullable *time.Time.
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

func NewNullTime(t *time.Time) *NullTime {
	if t == nil {
		return &NullTime{Valid: false}
	}
	return &NullTime{Time: *t, Valid: true}
}

// Scan reads an LTX timestamp into t.
func (n *NullTime) Scan(src any) error {
	if src == nil {
		*n = NullTime{Time: time.Time{}, Valid: false}
		return nil
	}

	switch src := src.(type) {
	case string:
		v, err := ltx.ParseTimestamp(src)
		if err != nil {
			return err
		}
		*n = NullTime{Time: v.UTC(), Valid: true}
		return nil
	default:
		return fmt.Errorf("invalid time format: %T", src)
	}
}

// Value returns the time in LTX timestamp format.
func (n NullTime) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return ltx.FormatTimestamp(n.Time.UTC()), nil
}

// T returns the value as a *time.Time
func (n NullTime) T() *time.Time {
	if !n.Valid {
		return nil
	}
	t := n.Time // make a shallow copy & return that pointer
	return &t
}

// Checksum represents an LTX checksum that is stored as a BLOB in SQLite.
type Checksum uint64

// Scan an 8-byte BLOB as a checksum. Zero value is encoded as a NULL.
func (v *Checksum) Scan(src any) error {
	if src == nil {
		*v = 0
		return nil
	}

	switch src := src.(type) {
	case []byte:
		if len(src) != 8 {
			return fmt.Errorf("invalid ltx checksum length: %d", len(src))
		}
		*v = Checksum(binary.BigEndian.Uint64(src))
		return nil
	default:
		return fmt.Errorf("invalid checksum format: %T", src)
	}
}

// Value returns the time in RFC 3339 format.
func (v Checksum) Value() (driver.Value, error) {
	if v == 0 {
		return nil, nil
	}

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b, nil
}
