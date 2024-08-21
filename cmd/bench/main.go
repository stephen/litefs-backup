package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/stephen/litefs-backup/internal/testingutil"
	"github.com/superfly/ltx"
	"golang.org/x/exp/constraints"
	"golang.org/x/sync/errgroup"
)

func main() {
	if err := NewBenchCommand().Run(context.Background(), os.Args[1:]); err != nil {
		panic(err)
	}
}

const (
	databasePrefix = "db"
	maxDBSize      = 25600 // in pages
	pageSize       = 4096
	pagesPerTx     = 2
)

// BenchCommand represents a command to benchmark LFSC.
type BenchCommand struct {
	bytesWritten atomic.Uint64
}

// NewBenchCommand returns a new instance of BenchCommand.
func NewBenchCommand() *BenchCommand {
	return &BenchCommand{}
}

// Run parses the command line flags, config, & executes the command.
func (c *BenchCommand) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("lfsc-bench", flag.ContinueOnError)
	baseURL := fs.String("url", "http://localhost:2200", "LiteFS Cloud URL")
	cluster := fs.String("cluster", "", "LiteFS cluster name")
	clusterID := fs.String("cluster-id", "", "LiteFS cluster id")
	databases := fs.Uint("databases", 1, "number of test databases in the cluster")
	token := fs.String("token", "", "authorization token")
	seed := fs.Int64("seed", 0, "random seed")
	fs.Usage = func() {
		fmt.Println(`
The bench command runs a benchmark against a LFSC instance and periodically
prints throughput statistics.

Usage:

	lfsc bench [arguments]

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println("")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *baseURL == "" {
		return fmt.Errorf("required: -url URL")
	} else if *clusterID == "" {
		return fmt.Errorf("-cluster-id can't be an empty string")
	} else if *databases == 0 {
		return fmt.Errorf("-databases can't be zero")
	}

	// Initialize logger.
	slog.SetDefault(slog.New(
		slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
	))

	client, err := c.makeClient(*baseURL, *cluster, *clusterID, *token)
	if err != nil {
		return fmt.Errorf("failed to create LFSC clients: %w", err)
	}

	slog.Info("syncing last databases positions with LFSC")

	initialPositions, err := c.syncInitialPositions(ctx, client, *databases)
	if err != nil {
		return fmt.Errorf("failed to read initial database positions: %w", err)
	}

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(*seed))

	slog.Info("starting benchmark",
		slog.String("cluster", *cluster),
		slog.Int("databases", int(*databases)),
		slog.Int64("seed", *seed),
	)
	slog.Info("terminate with ctrl-c")

	return c.runBenchmark(ctx, client, rng, initialPositions, *databases)
}

func (c *BenchCommand) databaseName(idx uint) string {
	return fmt.Sprintf("%s%d", databasePrefix, idx)
}

func (c *BenchCommand) makeClient(baseURL string, cluster string, clusterID string, token string) (*testingutil.Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	client, err := testingutil.NewClient(*u)
	if err != nil {
		return nil, err
	}
	client.Cluster = cluster
	client.ClusterID = clusterID
	client.AuthToken = token

	return client, nil
}

func (c *BenchCommand) syncInitialPositions(
	ctx context.Context,
	client *testingutil.Client,
	databases uint,
) (map[string]ltx.Pos, error) {
	pos, err := client.PosMap(ctx)
	if err != nil {
		return nil, err
	}

	return pos, nil
}

func (c *BenchCommand) runBenchmark(
	ctx context.Context,
	client *testingutil.Client,
	rng *rand.Rand,
	initialPositions map[string]ltx.Pos,
	databases uint,
) error {
	g, groupCtx := errgroup.WithContext(ctx)

	for i := uint(0); i < databases; i++ {
		i := i
		pos := initialPositions[c.databaseName(i)]

		rng := rand.New(rand.NewSource(rng.Int63()))
		bench := &dbBench{
			client:            client,
			rng:               rng,
			bytesWritten:      &c.bytesWritten,
			database:          c.databaseName(i),
			lastTXID:          pos.TXID,
			postApplyChecksum: pos.PostApplyChecksum,
		}
		g.Go(func() error {
			return bench.run(groupCtx)
		})
	}

	errc := make(chan error, 1)
	go func() {
		errc <- g.Wait()
	}()

	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()

	lastReport := time.Now()
	var lastWritten uint64
	for {
		select {
		case err := <-errc:
			if ctx.Err() != nil {
				err = nil
			}
			return err

		case <-statsTicker.C:
			written := c.bytesWritten.Load()
			now := time.Now()
			fmt.Printf(
				"%s Sent: %.2fMb, throughput: %.2fMb/s\n",
				now.Format(time.RFC3339),
				float64(written-lastWritten)/1024/1024,
				float64(written-lastWritten)/now.Sub(lastReport).Seconds()/1024/1024,
			)

			lastReport = now
			lastWritten = written
		}
	}
}

func min[T constraints.Ordered](first T, nums ...T) T {
	m := first
	for _, n := range nums {
		if n < m {
			m = n
		}
	}

	return m
}

type dbBench struct {
	client            *testingutil.Client
	rng               *rand.Rand
	bytesWritten      *atomic.Uint64
	database          string
	lastTXID          ltx.TXID
	postApplyChecksum ltx.Checksum
}

func (b *dbBench) run(ctx context.Context) (err error) {
	var buf bytes.Buffer
	for ctx.Err() == nil {
		var pageNums []uint32
		if b.lastTXID == 0 {
			pageNums = b.getSnapshotPageNums()
		} else {
			pageNums = b.getDeltaPageNums()
		}

		if err := b.makeLTX(&buf, pageNums); err != nil {
			return fmt.Errorf("encode ltx (%s): %w", b.database, err)
		}

		if err := b.client.WriteTx(ctx, b.database, &buf); err != nil {
			return fmt.Errorf("write tx (%s): %w", b.database, err)
		}

		buf.Reset()

		b.bytesWritten.Add(pageSize * pagesPerTx)
	}

	return ctx.Err()
}

func (b *dbBench) getSnapshotPageNums() []uint32 {
	pageNums := make([]uint32, 0, pagesPerTx)
	for i := 0; i < pagesPerTx; i++ {
		pageNums = append(pageNums, uint32(i+1))
	}
	return pageNums
}

func (b *dbBench) getDeltaPageNums() []uint32 {
	// We need to make sure that when all LTXs are compacted, the page numbers
	// are sequential. So use TXID as a hint how many pages the DB has. It has
	// min(pagesPerTx * TXID, maxDBSize)
	//
	// We will write new pages up until the DB reaches max DB size and start
	// exclusively rewriting existing ones after that.
	lastPage := uint32(min(ltx.TXID(maxDBSize), b.lastTXID*pagesPerTx))
	pageNums := make([]uint32, 0, pagesPerTx)
	if lastPage+pagesPerTx <= maxDBSize {
		for i := 1; i <= pagesPerTx; i++ {
			pageNums = append(pageNums, lastPage+uint32(i))
		}
	} else {
		startPage := b.rng.Intn(maxDBSize - 64)
		for _, pgNum := range b.rng.Perm(64)[:pagesPerTx] {
			pageNums = append(pageNums, uint32(startPage+pgNum))
		}
	}

	sort.Slice(pageNums, func(i, j int) bool { return pageNums[i] < pageNums[j] })

	return pageNums
}

func (b *dbBench) makeLTX(w io.Writer, pageNums []uint32) error {
	enc := ltx.NewEncoder(w)

	var commit uint32
	if (b.lastTXID+1)*pagesPerTx > maxDBSize {
		commit = maxDBSize
	} else {
		commit = pageNums[len(pageNums)-1]
	}

	if err := enc.EncodeHeader(ltx.Header{
		Version:          1,
		PageSize:         pageSize,
		Commit:           commit,
		MinTXID:          b.lastTXID + 1,
		MaxTXID:          b.lastTXID + 1,
		Timestamp:        time.Now().UnixMilli(),
		PreApplyChecksum: b.postApplyChecksum,
	}); err != nil {
		return fmt.Errorf("encode header: %w", err)
	}

	page := make([]byte, 4096)
	for i := 0; i < len(pageNums); i++ {
		_, _ = b.rng.Read(page)
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pageNums[i]}, page); err != nil {
			return fmt.Errorf("encode page (%d): %w", i, err)
		}
	}

	// We don't really care for as long as pre apply == previous post apply
	checksum := ltx.Checksum(b.rng.Uint64()) | ltx.ChecksumFlag
	enc.SetPostApplyChecksum(checksum)

	if err := enc.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	b.lastTXID += 1
	b.postApplyChecksum = checksum

	return nil
}
