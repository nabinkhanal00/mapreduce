package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/nabinkhanal00/labs/mapreduce"
	"github.com/nabinkhanal00/labs/wordcount"
)

func main() {
	coord := flag.String("coordinator", "", "master coordinator address (host:port)")
	logLevel := flag.String("log-level", "info", "log level (debug, info, warn, error)")
	health := flag.Duration("healthcheck", 10*time.Second, "health check interval")
	wait := flag.Duration("task-wait", 2*time.Second, "time to wait when no task is available")
	flag.Parse()

	if *coord == "" {
		fatal(" -coordinator is required")
	}

	logger := setupLogger(*logLevel)
	wordcount.Register()

	w, err := mapreduce.NewWorker(*coord, &mapreduce.WorkerOpts{
		HealthCheckTime: *health,
		TaskWaitTime:    *wait,
		Logger:          logger,
	})
	if err != nil {
		fatal(err.Error())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := w.Run(ctx); err != nil {
		if ctx.Err() != nil {
			logger.Info("worker stopped by signal", "worker_id", w.ID)
			return
		}
		logger.Error("worker run failed", "worker_id", w.ID, "err", err)
		os.Exit(1)
	}
	logger.Info("worker finished", "worker_id", w.ID)
}

func setupLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}

func fatal(msg string) {
	slog.Error(msg)
	os.Exit(1)
}
