package lfsb_test

import (
	"flag"
	"fmt"
	"testing"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/superfly/ltx"
)

var _ = flag.Bool("integration", false, "run integration tests")

func TestErrorCode(t *testing.T) {
	for _, tt := range []struct {
		err  error
		want string
	}{
		// Root error is lfsb.Error
		{lfsb.Errorf(lfsb.ErrorTypeValidation, "EXYZ", "test"), "EXYZ"},

		// Nested lfsb.Error
		{fmt.Errorf("test: %w", lfsb.Errorf(lfsb.ErrorTypeValidation, "EXYZ", "...")), "EXYZ"},

		// Non-lfsb.Error
		{fmt.Errorf("test"), "EINTERNAL"},

		// Nil errors
		{nil, ""},
		{fmt.Errorf("test: %w", nil), "EINTERNAL"},
	} {
		t.Run("", func(t *testing.T) {
			if got, want := lfsb.ErrorCode(tt.err), tt.want; got != want {
				t.Fatalf("code=%q, want %q", got, want)
			}
		})
	}
}

func TestValidateClusterName(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if err := lfsb.ValidateClusterName("cluster"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrClusterRequired", func(t *testing.T) {
		if err := lfsb.ValidateClusterName(""); err != lfsb.ErrClusterRequired {
			t.Fatal(err)
		}
	})
	t.Run("ErrClusterInvalid", func(t *testing.T) {
		if err := lfsb.ValidateClusterName("foo bar"); err != lfsb.ErrClusterInvalid {
			t.Fatal(err)
		}
	})
}

func TestValidateDatabase(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if err := lfsb.ValidateDatabase("database"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ErrDatabaseRequired", func(t *testing.T) {
		if err := lfsb.ValidateDatabase(""); err != lfsb.ErrDatabaseRequired {
			t.Fatal(err)
		}
	})
	t.Run("ErrDatabaseInvalid", func(t *testing.T) {
		if err := lfsb.ValidateDatabase("foo bar"); lfsb.ErrorCode(err) != "EBADDB" {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestParseLTXFilename(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		for i, tt := range []struct {
			name     string
			min, max ltx.TXID
		}{
			{name: "00000000000003e8-00000000000007d0.ltx", min: 1000, max: 2000},
			{name: "0000000000000001-0000000000000001.ltx", min: 1, max: 1},
		} {
			if min, max, err := lfsb.ParseLTXFilename(tt.name); err != nil {
				t.Error(i, err)
			} else if min != tt.min {
				t.Errorf("%d. min=%x, want %x", i, min, tt.min)
			} else if max != tt.max {
				t.Errorf("%d. max=%x, want %x", i, max, tt.max)
			}
		}
	})

	t.Run("ErrInvalidLTXFilename", func(t *testing.T) {
		for i, tt := range []struct {
			name string
		}{
			{name: "0000000000000001.ltx"},                  // too short
			{name: "0000000000000001-0000000000000001.xyz"}, // invalid extension
			{name: "000000000000000X-0000000000000001.ltx"}, // invalid min txid
			{name: "0000000000000001-000000000000000X.ltx"}, // invalid max txid
		} {
			if _, _, err := lfsb.ParseLTXFilename(tt.name); err != lfsb.ErrInvalidLTXFilename {
				t.Errorf("%d. unexpected error: %s", i, err)
			}
		}
	})
}
