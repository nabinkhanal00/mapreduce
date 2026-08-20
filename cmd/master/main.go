package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/nabinkhanal00/labs/mapreduce"
	"github.com/nabinkhanal00/labs/wordcount"
)

const maxJobs = 100

func main() {
	var (
		logLevel  = flag.String("log-level", "info", "log level (debug, info, warn, error)")
		coordAddr = flag.String("coordinator", "0.0.0.0:39000", "address the coordinator RPC listens on")
		httpAddr  = flag.String("http", "0.0.0.0:8080", "address of the job submission HTTP API")
		fsAddr    = flag.String("fs", "", "file server address (host:port) used to read/write files")
		timeout   = flag.Duration("timeout", 10*time.Second, "task execution timeout")
	)
	flag.Parse()

	logger := setupLogger(*logLevel)
	wordcount.Register()

	if *fsAddr == "" {
		fatal(logger, "-fs file server address is required (start cmd/fileserver first)")
	}

	coord := mapreduce.NewCoordinator(*fsAddr, *timeout)
	coord.SetLogger(logger)

	listener, err := coord.ServeOn(*coordAddr)
	if err != nil {
		fatal(logger, "coordinator RPC listen failed", "addr", *coordAddr, "err", err)
	}

	daemon := &jobDaemon{
		coord:  coord,
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/submit", daemon.handleSubmit)
	mux.HandleFunc("/api/v1/jobs", daemon.handleJobs)
	httpSrv := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		logger.Info("job HTTP API listening", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("job HTTP API failed", "err", err)
		}
	}()

	logger.Info("master daemon started",
		"coordinator", coord.Addr(),
		"http", httpSrv.Addr,
		"file_server", *fsAddr,
		"timeout", timeout.String(),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	logger.Info("shutdown signal received, stopping master")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	_ = listener.Close()
	logger.Info("master stopped")
}

type JobInfo struct {
	ID          int                     `json:"id"`
	Phase       string                  `json:"phase"`
	Spec        mapreduce.Specification `json:"spec"`
	Started     time.Time               `json:"started"`
	Finished    time.Time               `json:"finished"`
	Elapsed     time.Duration           `json:"elapsed"`
	Counters    map[string]int64        `json:"counters"`
	OutputFiles []string                `json:"output_files"`
	Error       string                  `json:"error,omitempty"`
}

type jobDaemon struct {
	coord  *mapreduce.Coordinator
	logger *slog.Logger

	mu   sync.Mutex
	jobs []*JobInfo
	seq  int
}

type submitRequest struct {
	Spec            mapreduce.Specification `json:"spec"`
	IntermediateDir string                  `json:"intermediate_dir"`
}

func (d *jobDaemon) handleSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.IntermediateDir == "" {
		req.IntermediateDir = "intermediate"
	}

	resultCh, err := d.coord.SubmitJob(req.Spec, req.IntermediateDir)
	if err != nil {
		d.logger.Warn("job submission rejected", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := d.recordStarted(req.Spec, req.IntermediateDir)
	d.logger.Info("job submitted via API", "job_id", id, "intermediate_dir", req.IntermediateDir)
	go func(jobID int, results <-chan mapreduce.Result) {
		res := <-results
		d.recordFinished(jobID, res)
	}(id, resultCh)

	writeJSON(w, http.StatusAccepted, map[string]int{"job_id": id})
}

func (d *jobDaemon) handleJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"jobs": d.jobs})
}

func (d *jobDaemon) recordStarted(spec mapreduce.Specification, interDir string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	job := &JobInfo{
		ID:      d.seq,
		Phase:   "running",
		Spec:    spec,
		Started: time.Now(),
	}
	d.jobs = append(d.jobs, job)
	if len(d.jobs) > maxJobs {
		d.jobs = d.jobs[len(d.jobs)-maxJobs:]
	}
	return d.seq
}

func (d *jobDaemon) recordFinished(id int, res mapreduce.Result) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, job := range d.jobs {
		if job.ID == id {
			job.Phase = "done"
			job.Finished = time.Now()
			job.Elapsed = res.Elapsed
			job.Counters = res.Counters
			job.OutputFiles = res.OutputFiles
			d.logger.Info("job recorded as finished",
				"job_id", id,
				"elapsed", res.Elapsed.String(),
				"counters", res.Counters,
			)
			return
		}
	}
	d.logger.Warn("finished job not found in history", "job_id", id)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
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

func fatal(logger *slog.Logger, msg string, args ...any) {
	logger.Error(msg, args...)
	os.Exit(1)
}
