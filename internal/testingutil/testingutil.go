package testingutil

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/superfly/ltx"
)

// StatefulBlob represents a binary blob that LTX changesets can be generated from.
type StatefulBlob struct {
	pos      ltx.Pos
	data     []byte
	rand     *rand.Rand
	pageSize uint32
}

// NewStatefulBlob returns a zero-length StatefulBlob.
func NewStatefulBlob(seed int64, pageSize uint32) *StatefulBlob {
	return &StatefulBlob{
		data:     make([]byte, 0),
		rand:     rand.New(rand.NewSource(seed)),
		pageSize: pageSize,
	}
}

// Pos returns the current replication position.
func (b *StatefulBlob) Pos() ltx.Pos { return b.pos }

// Commit returns the number of pages in the blob.
func (b *StatefulBlob) Commit() uint32 {
	return uint32(len(b.data)) / b.pageSize
}

// Clone returns a copy of the entire state of the blob with a new PRNG.
func (b *StatefulBlob) Clone() *StatefulBlob {
	other := &StatefulBlob{
		pos:      b.pos,
		data:     make([]byte, len(b.data)),
		rand:     rand.New(rand.NewSource(b.rand.Int63())),
		pageSize: b.pageSize,
	}
	copy(other.data, b.data)
	return other
}

// Extend adds pages to change b to the given size. Fatal if n is less than len.
// Returns a reader of LTX data with the changes.
func (b *StatefulBlob) Extend(tb testing.TB, n int64) io.Reader {
	tb.Helper()
	if n%int64(b.pageSize) != 0 {
		tb.Fatalf("size must be a multiple of page size")
	} else if n <= int64(len(b.data)) {
		tb.Fatalf("cannot shrink blob from %d bytes to %d bytes", len(b.data), n)
	}

	prevCommit := b.Commit()
	commit := uint32(n) / b.pageSize

	var buf bytes.Buffer
	enc := ltx.NewEncoder(&buf)
	if err := enc.EncodeHeader(ltx.Header{
		Version:          ltx.Version,
		PageSize:         b.pageSize,
		Commit:           commit,
		MinTXID:          b.pos.TXID + 1,
		MaxTXID:          b.pos.TXID + 1,
		Timestamp:        0,
		PreApplyChecksum: b.pos.PostApplyChecksum,
	}); err != nil {
		tb.Fatalf("cannot encode header: %s", err)
	}

	postApplyChecksum := b.pos.PostApplyChecksum
	for pgno := prevCommit + 1; pgno <= commit; pgno++ {
		pageData := make([]byte, b.pageSize)

		if pgno != ltx.LockPgno(b.pageSize) {
			_, _ = b.rand.Read(pageData)
			postApplyChecksum = ltx.ChecksumFlag | (postApplyChecksum ^ ltx.ChecksumPage(pgno, pageData))
		}

		b.data = append(b.data, pageData...)

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageData); err != nil {
			tb.Fatalf("cannot encode page %d: %s", pgno, err)
		}
	}

	enc.SetPostApplyChecksum(postApplyChecksum)
	if err := enc.Close(); err != nil {
		tb.Fatalf("cannot close ltx encoder: %s", err)
	}

	b.pos = ltx.Pos{
		TXID:              b.pos.TXID + 1,
		PostApplyChecksum: postApplyChecksum,
	}

	return &buf
}
