package mapreduce

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/denisbrodbeck/machineid"
)

type Worker struct {
	ID            string
	MasterAddress netip.AddrPort
	logger        *slog.Logger
	currentTask   Task
	tasklock      sync.RWMutex
	*WorkerOpts
}

type WorkerOpts struct {
	HealthCheckTime time.Duration
	TaskWaitTime    time.Duration
}

var DefaultWorkerOpts = &WorkerOpts{
	HealthCheckTime: 10 * time.Second,
	TaskWaitTime:    10 * time.Second,
}

func NewWorker(coordAddr string, opts *WorkerOpts) (*Worker, error) {
	id, err := machineid.ID()
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
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))
	return &Worker{
		ID:            id,
		MasterAddress: addr,
		logger:        logger,
		WorkerOpts:    opts,
	}, nil
}

func (w *Worker) performHealthChecks(check func() (bool, error), ctx context.Context, cancel context.CancelFunc) {
	ticker := time.NewTicker(w.HealthCheckTime)
	defer ticker.Stop()
	for {
		counter := 0
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stop, err := check()
			if err != nil {
				w.logger.Warn("mapreduce: HealthCheck", slog.String("status", "failed"))
				counter++
				if counter > 10 {
					cancel()
					return
				}
			}
			if stop {
				cancel()
				return
			}
		}
	}
}

func (w *Worker) Run(ctx context.Context) error {
	client, err := rpc.Dial("tcp", w.MasterAddress.String())
	if err != nil {
		return fmt.Errorf("mapreduce: Connection: worker dial %s: %w", w.MasterAddress.String(), err)
	}
	sendHealthCheck := func() (bool, error) {
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

	shouldStopHealthcheck := make(chan struct{})
	// stop sending healthcheck when program returns
	defer func() { shouldStopHealthcheck <- struct{}{} }()

	workerCtx, cancel := context.WithCancel(ctx)

	go w.performHealthChecks(sendHealthCheck, workerCtx, cancel)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var reply GetTaskReply
		if err := client.Call("Coordinator.GetTask", &GetTaskArgs{HostID: w.ID}, &reply); err != nil {
			return fmt.Errorf("mapreduce: GetTask: %w", err)
		}
		if reply.Done {
			return nil
		}
		if reply.Task == nil {
			time.Sleep(w.TaskWaitTime)
			continue
		}
		task := reply.Task
		w.setCurrentTask(task)
		report := ReportArgs{
			TaskID: task.GetID(),
			Type:   task.Type(),
			HostID: w.ID,
		}
		taskCtx, taskCancel := context.WithCancel(workerCtx)
		if err := task.Execute(taskCtx); err != nil {
			report.Err = err.Error()
		}
		taskCancel()
		w.currentTask = nil
		if err := client.Call("Coordinator.ReportTask", &report, nil); err != nil {
			return fmt.Errorf("mapreduce: ReportTask: %w", err)
		}

	}
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
