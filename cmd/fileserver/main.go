package main

import (
	"encoding/binary"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nabinkhanal00/labs/mapreduce"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5555", "address to listen on")
	logLevel := flag.String("log-level", "info", "log level (debug, info, warn, error)")
	flag.Parse()

	logger := setupLogger(*logLevel)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("file server listen failed", "addr", *addr, "err", err)
		os.Exit(1)
	}
	logger.Info("file server listening", "addr", listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Warn("file server accept failed", "err", err)
			return
		}
		go serve(logger, conn)
	}
}

func serve(logger *slog.Logger, conn net.Conn) {
	peer := conn.RemoteAddr().String()
	defer func() {
		_ = conn.Close()
		logger.Debug("connection closed", "peer", peer)
	}()
	logger.Debug("connection accepted", "peer", peer)

	for {
		var req mapreduce.RequestType
		if err := binary.Read(conn, binary.BigEndian, &req); err != nil {
			if err != io.EOF {
				logger.Warn("read request type failed", "peer", peer, "err", err)
			}
			return
		}
		var nameLen uint16
		if err := binary.Read(conn, binary.BigEndian, &nameLen); err != nil {
			logger.Warn("read filename length failed", "peer", peer, "err", err)
			return
		}
		name := make([]byte, nameLen)
		if _, err := io.ReadFull(conn, name); err != nil {
			logger.Warn("read filename failed", "peer", peer, "err", err)
			return
		}
		filename := string(name)

		start := time.Now()
		var err error
		switch req {
		case mapreduce.RequestTypeRead:
			err = sendFile(conn, filename)
		case mapreduce.RequestTypeWrite:
			err = recvFile(conn, filename)
		default:
			logger.Warn("invalid request type", "peer", peer, "request_type", req)
			return
		}
		if err != nil {
			logger.Warn("request failed",
				"peer", peer,
				"op", req.String(),
				"filename", filename,
				"duration", time.Since(start).String(),
				"err", err,
			)
			return
		}
		logger.Info("request served",
			"peer", peer,
			"op", req.String(),
			"filename", filename,
			"duration", time.Since(start).String(),
		)
	}
}

func sendFile(conn net.Conn, name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if err := binary.Write(conn, binary.BigEndian, uint64(stat.Size())); err != nil {
		return err
	}
	_, err = io.CopyN(conn, f, stat.Size())
	return err
}

func recvFile(conn net.Conn, name string) error {
	var size uint64
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		return err
	}
	if dir := filepath.Dir(name); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.Create(name)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.CopyN(f, conn, int64(size))
	return err
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
