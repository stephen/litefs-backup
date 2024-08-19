package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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

	if err := server.Run(ctx); err != nil {
		return err
	}

	slog.Info("waiting for signal or subprocess to exit")
	<-ctx.Done()
	slog.Info("signal received, shutting down")
	return nil
}
