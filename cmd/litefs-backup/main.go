package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/server"
)

func main() {
	if err := Run(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}
}

func Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	slog.InfoContext(ctx, "running litefs-backup", slog.String("version", lfsb.Version))

	config, err := lfsb.ConfigFromEnv()
	if err != nil {
		return err
	}

	if dsn := config.SentryDSN; dsn != "" {
		if err := sentry.Init(sentry.ClientOptions{Dsn: dsn, Debug: true}); err != nil {
			return fmt.Errorf("cannot init sentry: %w", err)
		}
		defer sentry.Flush(1 * time.Second)
	}

	if err := server.Run(ctx, config); err != nil {
		return err
	}

	slog.Info("waiting for signal or subprocess to exit")
	<-ctx.Done()
	slog.Info("signal received, shutting down")
	return nil
}
