package mapreduce

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/netip"
	"net/rpc"
	"sync"
	"time"
)

// Worker is a long-lived process that continuously polls the coordinator
// for tasks. It survives coordinator restarts by reconnecting with
// backoff, and it never exits on its own: it only stops when its context
// is canceled.
type Worker struct {
	ID            string
	MasterAddress netip.AddrPort
	logger        *slog.Logger
	currentTask   Task
	tasklock      sync.RWMutex
	client        *rpc.Client
	clientMu      sync.Mutex
	*WorkerOpts
}

type WorkerOpts struct {
	HealthCheckTime time.Duration
	TaskWaitTime    time.Duration
	Logger          *slog.Logger
}

var DefaultWorkerOpts = &WorkerOpts{
	HealthCheckTime: 10 * time.Second,
	TaskWaitTime:    2 * time.Second,
}

func NewWorker(coordAddr string, opts *WorkerOpts) (*Worker, error) {
	id, err := uniqueID()
	if err != nil {
		return nil, err
	}
	addr, err := netip.ParseAddrPort(coordAddr)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = DefaultWorkerOpts
	}
	logger := opts.Logger
	if logger == nil {
		logger = defaultLogger()
	}
	w := &Worker{
		ID:            id,
		MasterAddress: addr,
		logger:        logger,
		WorkerOpts:    opts,
	}
	w.logger.Info("worker initialized",
		"worker_id", w.ID,
		"coordinator", w.MasterAddress.String(),
		"health_check_interval", w.HealthCheckTime.String(),
		"task_wait_time", w.TaskWaitTime.String(),
	)
	return w, nil
}

// Run blocks until ctx is canceled, processing tasks across any number of
// jobs submitted to the coordinator.
func (w *Worker) Run(ctx context.Context) error {
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The health check goroutine uses its own connection so that the main
	// loop can reconnect without tearing it down.
	hcClient, err := w.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = hcClient.Close() }()
	go w.performHealthChecks(hcClient, workerCtx, cancel)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		client := w.getClient()
		if client == nil {
			c, err := w.dial(ctx)
			if err != nil {
				return err
			}
			w.setClient(c)
			client = c
		}
		if err := w.workOnce(ctx, client); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.logger.Warn("worker connection lost, reconnecting",
				"worker_id", w.ID,
				"err", err,
			)
			w.closeClient()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(w.TaskWaitTime):
			}
		}
	}
}

func (w *Worker) dial(ctx context.Context) (*rpc.Client, error) {
	for {
		w.logger.Info("connecting to coordinator",
			"worker_id", w.ID,
			"coordinator", w.MasterAddress.String(),
		)
		c, err := rpc.Dial("tcp", w.MasterAddress.String())
		if err == nil {
			w.logger.Info("connected to coordinator",
				"worker_id", w.ID,
				"coordinator", w.MasterAddress.String(),
			)
			return c, nil
		}
		w.logger.Warn("coordinator dial failed, retrying",
			"worker_id", w.ID,
			"err", err,
		)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(w.TaskWaitTime):
		}
	}
}

// workOnce fetches and executes a single task, or waits when there is
// nothing to do. It returns an error only for connection failures so the
// caller can reconnect.
func (w *Worker) workOnce(ctx context.Context, client *rpc.Client) error {
	var reply GetTaskReply
	if err := client.Call("Coordinator.GetTask", &GetTaskArgs{HostID: w.ID}, &reply); err != nil {
		return fmt.Errorf("mapreduce: GetTask: %w", err)
	}
	if reply.Done {
		w.logger.Info("coordinator signalled shutdown", "worker_id", w.ID)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.TaskWaitTime):
		}
		return nil
	}
	if reply.Task == nil {
		w.logger.Debug("no task available, waiting",
			"worker_id", w.ID,
			"wait", w.TaskWaitTime.String(),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.TaskWaitTime):
		}
		return nil
	}

	task := reply.Task
	w.setCurrentTask(task)
	report := ReportArgs{
		TaskID: task.GetID(),
		Type:   task.Type(),
		HostID: w.ID,
	}
	w.logger.Info("executing task",
		"worker_id", w.ID,
		"task_id", task.GetID(),
		"task_type", task.Type().String(),
	)
	taskCtx, taskCancel := context.WithCancel(ctx)
	start := time.Now()
	if err := task.Execute(taskCtx); err != nil {
		report.Err = err.Error()
		w.logger.Warn("task execution failed",
			"worker_id", w.ID,
			"task_id", task.GetID(),
			"task_type", task.Type().String(),
			"duration", time.Since(start).String(),
			"err", err,
		)
	} else {
		w.logger.Info("task execution succeeded",
			"worker_id", w.ID,
			"task_id", task.GetID(),
			"task_type", task.Type().String(),
			"duration", time.Since(start).String(),
		)
	}
	taskCancel()
	w.clearCurrentTask()
	if err := client.Call("Coordinator.ReportTask", &report, nil); err != nil {
		return fmt.Errorf("mapreduce: ReportTask: %w", err)
	}
	w.logger.Info("reported task result",
		"worker_id", w.ID,
		"task_id", report.TaskID,
		"task_type", report.Type.String(),
		"err", report.Err,
	)
	return nil
}

func (w *Worker) performHealthChecks(client *rpc.Client, ctx context.Context, cancel context.CancelFunc) {
	ticker := time.NewTicker(w.HealthCheckTime)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("health check loop stopped", "reason", "context canceled")
			return
		case <-ticker.C:
			stop, err := w.sendHealthCheck(client)
			if err != nil {
				w.logger.Warn("health check failed",
					"worker_id", w.ID,
					"err", err,
				)
			}
			if stop {
				w.logger.Warn("coordinator requested worker stop", "worker_id", w.ID)
				cancel()
				return
			}
		}
	}
}

func (w *Worker) sendHealthCheck(client *rpc.Client) (bool, error) {
	taskID := ""
	taskType := NoneType
	task := w.getCurrentTask()
	if task != nil {
		taskID = task.GetID()
		taskType = task.Type()
	}
	args := &HealthCheckArgs{
		HostID: w.ID,
		TaskID: taskID,
		Type:   taskType,
	}
	reply := &HealthCheckReply{}

	err := client.Call("Coordinator.HealthCheck", args, reply)
	return reply.Stop, err
}

func (w *Worker) getClient() *rpc.Client {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	return w.client
}

func (w *Worker) setClient(c *rpc.Client) {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	w.client = c
}

func (w *Worker) closeClient() {
	w.clientMu.Lock()
	defer w.clientMu.Unlock()
	if w.client != nil {
		_ = w.client.Close()
		w.client = nil
	}
}

// uniqueID returns a random identifier unique to this worker process.
func uniqueID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (w *Worker) getCurrentTask() Task {
	w.tasklock.RLock()
	defer w.tasklock.RUnlock()
	return w.currentTask
}

func (w *Worker) setCurrentTask(task Task) {
	w.tasklock.Lock()
	defer w.tasklock.Unlock()
	w.currentTask = task
}

func (w *Worker) clearCurrentTask() {
	w.tasklock.Lock()
	defer w.tasklock.Unlock()
	w.currentTask = nil
}
