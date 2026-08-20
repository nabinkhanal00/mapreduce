package mapreduce

import (
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func Run(spec Specification) {
	_ = spec.Output.NumTasks
}

type taskState struct {
	task        Task
	status      TaskStatus
	deadline    time.Time
	healthCheck time.Time
	attempts    int
	hostid      string
}

type phase int

const (
	phaseIdle phase = iota
	phaseMap
	phaseReduce
	phaseDone
)

func (p phase) String() string {
	switch p {
	case phaseIdle:
		return "idle"
	case phaseMap:
		return "map"
	case phaseReduce:
		return "reduce"
	case phaseDone:
		return "done"
	default:
		return "unknown"
	}
}

// Coordinator is the master node. It is long-lived: workers keep polling
// it for tasks, and jobs are submitted through SubmitJob, which resets the
// coordinator for the next job while the RPC listener stays up.
type Coordinator struct {
	mu   sync.Mutex
	spec Specification

	intermediateDir string
	addr            string

	mapTasks    map[string]*taskState
	reduceTasks map[string]*taskState

	phase    phase
	started  time.Time
	done     chan struct{}
	doneOnce sync.Once

	taskTimeout     time.Duration
	waitHealthCheck time.Duration

	result            Result
	currentResult     chan Result
	fileServerAddress string
	jobSeq            int

	logger *slog.Logger
}

// NewCoordinator creates an idle coordinator that has no job until
// SubmitJob is called. fileServerAddress is the address workers use to
// fetch input files and store intermediate and output files.
func NewCoordinator(fileServerAddress string, timeout time.Duration) *Coordinator {
	c := &Coordinator{
		fileServerAddress: fileServerAddress,
		done:              make(chan struct{}),
		phase:             phaseIdle,
		taskTimeout:       timeout,
		waitHealthCheck:   30 * time.Second,
		mapTasks:          make(map[string]*taskState),
		reduceTasks:       make(map[string]*taskState),
		logger:            defaultLogger(),
	}
	c.logger.Info("coordinator created",
		"file_server", fileServerAddress,
		"task_timeout", timeout.String(),
	)
	return c
}

// SetLogger sets the logger used for coordinator diagnostics. It must be
// called before the coordinator starts serving requests.
func (c *Coordinator) SetLogger(l *slog.Logger) {
	if l == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger = l
}

func defaultLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))
}

// SubmitJob validates a specification and starts it as a new job. It is
// rejected if a job is still running. The returned channel delivers the
// job's Result exactly once when the job finishes.
func (c *Coordinator) SubmitJob(spec Specification, intermediateDir string) (<-chan Result, error) {
	if spec.Output.NumTasks < 1 {
		return nil, fmt.Errorf("mapreduce: Output.NumTasks must be >= 1, got %d", spec.Output.NumTasks)
	}
	if spec.Output.Filebase == "" {
		return nil, fmt.Errorf("mapreduce: Output.Filebase must not be empty")
	}
	if spec.Output.Reducer == "" {
		return nil, fmt.Errorf("mapreduce: Output.Reducer must not be empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == phaseMap || c.phase == phaseReduce {
		return nil, fmt.Errorf("mapreduce: job still running (phase %s)", c.phase)
	}

	c.spec = spec
	c.intermediateDir = intermediateDir
	c.mapTasks = make(map[string]*taskState)
	c.reduceTasks = make(map[string]*taskState)
	c.phase = phaseMap
	c.started = time.Now()
	c.result = Result{}
	c.done = make(chan struct{})
	c.doneOnce = sync.Once{}
	c.currentResult = make(chan Result, 1)
	c.jobSeq++

	if err := c.buildTasks(); err != nil {
		c.phase = phaseIdle
		return nil, err
	}
	c.logger.Info("job submitted",
		"job_seq", c.jobSeq,
		"inputs", c.spec.Inputs,
		"output", c.spec.Output,
		"intermediate_dir", intermediateDir,
		"map_tasks", len(c.mapTasks),
		"reduce_tasks", len(c.reduceTasks),
	)
	return c.currentResult, nil
}

// CurrentPhase reports the coordinator phase (idle, map, reduce, done).
func (c *Coordinator) CurrentPhase() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase.String()
}

type discoveredInput struct {
	path   string
	format string
	mapper string
}

func (c *Coordinator) buildTasks() error {
	inputs := []discoveredInput{}
	for _, in := range c.spec.Inputs {
		format := in.Format
		if format == "" {
			format = "text"
		}
		if in.Mapper == "" {
			return fmt.Errorf("mapreduce: Input for pattern %q has no Mapper", in.FilePattern)
		}
		if _, err := lookupMapper(in.Mapper); err != nil {
			return err
		}

		matches, err := filepath.Glob(in.FilePattern)
		if err != nil {
			return fmt.Errorf("mapreduce: bad filepattern %q: %w", in.FilePattern, err)
		}
		for _, match := range matches {
			inputs = append(inputs, discoveredInput{path: match, format: format, mapper: in.Mapper})
			c.logger.Info("discovered input file",
				"path", match,
				"format", format,
				"mapper", in.Mapper,
			)
		}
	}
	if len(inputs) == 0 {
		return fmt.Errorf("mapreduce: no input files matched %v", c.spec.Inputs)
	}

	outFormat := c.spec.Output.Format
	if outFormat == "" {
		outFormat = "kv"
	}
	if _, err := lookupFormat(outFormat); err != nil {
		return err
	}
	if _, err := lookupReducer(c.spec.Output.Reducer); err != nil {
		return err
	}
	if c.spec.Output.Combiner != "" {
		if _, err := lookupReducer(c.spec.Output.Combiner); err != nil {
			return err
		}
	}
	for i, in := range inputs {
		id := fmt.Sprintf("%s-map-%d", c.spec.TaskPrefix, i)
		c.mapTasks[id] = &taskState{
			task: &MapTask{
				ID:          id,
				InputFile:   in.path,
				InputFormat: in.format,
				NumReduce:   c.spec.Output.NumTasks,
				Mapper:      in.mapper,
				Combiner:    c.spec.Output.Combiner,

				IntermediateDir:   c.intermediateDir,
				Format:            outFormat,
				FileSourceAddress: c.fileServerAddress,
			},
			status: StatusPending,
		}
	}
	for i := range c.spec.Output.NumTasks {
		id := fmt.Sprintf("%s-reduce-%05d", c.spec.TaskPrefix, i)
		c.reduceTasks[id] = &taskState{
			task: &ReduceTask{
				ID:            id,
				Bucket:        i,
				MapFilePrefix: fmt.Sprintf("%s-map", c.spec.TaskPrefix),
				NumMap:        len(c.mapTasks),
				Reducer:       c.spec.Output.Reducer,
				Combiner:      c.spec.Output.Combiner,
				OutputBase:    OutputName(c.spec.Output.Filebase, i),

				Format:            outFormat,
				IntermediateDir:   c.intermediateDir,
				FileSourceAddress: c.fileServerAddress,
			},
			status: StatusPending,
		}
	}
	return nil
}

func (c *Coordinator) Serve() (net.Listener, error) {
	return c.serve("0.0.0.0:0")
}

func (c *Coordinator) ServeOn(listenAddr string) (net.Listener, error) {
	return c.serve(listenAddr)
}

func (c *Coordinator) serve(listenAddr string) (net.Listener, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	server := rpc.NewServer()
	if err := server.RegisterName("Coordinator", c); err != nil {
		_ = listener.Close()
		return nil, err
	}
	go accept(server, listener, c.logger)
	c.addr = listener.Addr().String()
	c.logger.Info("coordinator serving",
		"addr", c.addr,
		"phase", c.phase.String(),
	)
	return listener, nil
}

func accept(server *rpc.Server, listener net.Listener, logger *slog.Logger) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.Warn("coordinator accept stopped", "err", err)
			return
		}
		go server.ServeConn(conn)
	}
}

func (c *Coordinator) Addr() string          { return c.addr }
func (c *Coordinator) Done() <-chan struct{} { return c.done }
func (c *Coordinator) Result() Result        { return c.result }

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	ts := c.findTaskLocked(args.TaskID, args.Type)
	if ts == nil || ts.status != StatusRunning || ts.hostid != args.HostID {
		c.logger.Warn("ignoring task report",
			"job_seq", c.jobSeq,
			"task_id", args.TaskID,
			"task_type", args.Type.String(),
			"host_id", args.HostID,
			"err", args.Err,
		)
		return nil
	}
	if args.Err != "" {
		ts.status = StatusPending
		ts.hostid = ""
		c.logger.Warn("task reported failure, requeued",
			"job_seq", c.jobSeq,
			"task_id", args.TaskID,
			"task_type", args.Type.String(),
			"host_id", args.HostID,
			"err", args.Err,
			"attempts", ts.attempts,
		)
		return nil
	}
	ts.status = StatusDone
	c.logger.Info("task completed",
		"job_seq", c.jobSeq,
		"task_id", args.TaskID,
		"task_type", args.Type.String(),
		"host_id", args.HostID,
		"phase", c.phase.String(),
		"attempts", ts.attempts,
	)

	switch c.phase {
	case phaseMap:
		if c.allDoneLocked(c.mapTasks) {
			c.logger.Info("all map tasks complete, starting reduce phase", "job_seq", c.jobSeq)
			c.startReduceLocked()
		}
	case phaseReduce:
		if c.allDoneLocked(c.reduceTasks) {
			c.logger.Info("all reduce tasks complete, finishing job", "job_seq", c.jobSeq)
			c.finishLocked()
		}
	}
	return nil
}

func (c *Coordinator) HealthCheck(args *HealthCheckArgs, reply *HealthCheckReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Type == NoneType {
		return nil
	}
	ts := c.findTaskLocked(args.TaskID, args.Type)
	if ts == nil || ts.hostid != args.HostID || ts.status != StatusRunning {
		reply.Stop = true
		c.logger.Warn("health check rejected, worker should stop",
			"job_seq", c.jobSeq,
			"task_id", args.TaskID,
			"task_type", args.Type.String(),
			"host_id", args.HostID,
		)
		return nil
	}
	ts.healthCheck = time.Now()
	c.logger.Debug("health check acknowledged",
		"job_seq", c.jobSeq,
		"task_id", args.TaskID,
		"task_type", args.Type.String(),
		"host_id", args.HostID,
	)
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.readExpiredLocked()

	switch c.phase {
	case phaseMap:
		if ts := c.pickPendingLocked(c.mapTasks); ts != nil {
			ts.hostid = args.HostID
			reply.Task = ts.task
			c.logger.Info("assigned map task",
				"job_seq", c.jobSeq,
				"task_id", ts.task.GetID(),
				"host_id", args.HostID,
				"attempts", ts.attempts,
			)
			return nil
		}
		if !c.allDoneLocked(c.mapTasks) {
			c.logger.Debug("no map task available",
				"job_seq", c.jobSeq,
				"host_id", args.HostID,
			)
			return nil
		}
		c.logger.Info("all map tasks complete, starting reduce phase", "job_seq", c.jobSeq)
		c.startReduceLocked()
		if ts := c.pickPendingLocked(c.reduceTasks); ts != nil {
			ts.hostid = args.HostID
			reply.Task = ts.task
			c.logger.Info("assigned reduce task",
				"job_seq", c.jobSeq,
				"task_id", ts.task.GetID(),
				"host_id", args.HostID,
				"attempts", ts.attempts,
			)
			return nil
		}
		return nil
	case phaseReduce:
		if ts := c.pickPendingLocked(c.reduceTasks); ts != nil {
			ts.hostid = args.HostID
			reply.Task = ts.task
			c.logger.Info("assigned reduce task",
				"job_seq", c.jobSeq,
				"task_id", ts.task.GetID(),
				"host_id", args.HostID,
				"attempts", ts.attempts,
			)
			return nil
		}
		if !c.allDoneLocked(c.reduceTasks) {
			c.logger.Debug("no reduce task available",
				"job_seq", c.jobSeq,
				"host_id", args.HostID,
			)
			return nil
		}
		c.finishLocked()
		return nil
	default:
		// idle or done: workers keep polling for the next job
		c.logger.Debug("no job active, worker should keep polling",
			"job_seq", c.jobSeq,
			"phase", c.phase.String(),
			"host_id", args.HostID,
		)
		return nil
	}
}

func (c *Coordinator) readExpiredLocked() {
	now := time.Now()
	for _, ts := range c.mapTasks {
		if ts.status == StatusRunning && (now.After(ts.deadline) || now.Sub(ts.healthCheck) > c.waitHealthCheck) {
			ts.status = StatusPending
			ts.hostid = ""
			c.logger.Warn("map task expired, requeued",
				"job_seq", c.jobSeq,
				"task_id", ts.task.GetID(),
				"deadline", ts.deadline.Format(time.RFC3339Nano),
				"attempts", ts.attempts,
			)
		}
	}
	for _, ts := range c.reduceTasks {
		if ts.status == StatusRunning && (now.After(ts.deadline) || now.Sub(ts.healthCheck) > c.waitHealthCheck) {
			ts.status = StatusPending
			ts.hostid = ""
			c.logger.Warn("reduce task expired, requeued",
				"job_seq", c.jobSeq,
				"task_id", ts.task.GetID(),
				"deadline", ts.deadline.Format(time.RFC3339Nano),
				"attempts", ts.attempts,
			)
		}
	}
}

func (c *Coordinator) pickPendingLocked(tasks map[string]*taskState) *taskState {
	for _, ts := range tasks {
		if ts.status == StatusPending {
			ts.status = StatusRunning
			ts.deadline = time.Now().Add(c.taskTimeout)
			ts.healthCheck = time.Now()
			ts.attempts++
			return ts
		}
	}
	return nil
}

func (c *Coordinator) allDoneLocked(tasks map[string]*taskState) bool {
	for _, ts := range tasks {
		if ts.status != StatusDone {
			return false
		}
	}
	return true
}

func (c *Coordinator) startReduceLocked() {
	c.phase = phaseReduce
}

func (c *Coordinator) finishLocked() {
	c.phase = phaseDone
	c.result = Result{
		Counters: map[string]int64{
			"map_tasks":              int64(len(c.mapTasks)),
			"reduce_tasks":           int64(len(c.reduceTasks)),
			"map_tasks_completed":    c.countDoneLocked(c.mapTasks),
			"reduce_tasks_completed": c.countDoneLocked(c.reduceTasks),
		},
		Elapsed:     time.Since(c.started),
		OutputFiles: c.outputFiles(),
	}
	c.logger.Info("job finished",
		"job_seq", c.jobSeq,
		"elapsed", c.result.Elapsed.String(),
		"counters", c.result.Counters,
		"output_files", c.result.OutputFiles,
	)
	c.doneOnce.Do(func() { close(c.done) })
	if c.currentResult != nil {
		c.currentResult <- c.result
	}
}

func (c *Coordinator) countDoneLocked(tasks map[string]*taskState) int64 {
	n := 0
	for _, ts := range tasks {
		if ts.status == StatusDone {
			n++
		}
	}
	return int64(n)
}

func (c *Coordinator) findTaskLocked(id string, tp TaskType) *taskState {
	if tp == MapType {
		v, ok := c.mapTasks[id]
		if !ok {
			return nil
		}
		return v
	}
	v, ok := c.reduceTasks[id]
	if !ok {
		return nil
	}
	return v
}

func (c *Coordinator) outputFiles() []string {
	var files []string
	for b := 0; b < c.spec.Output.NumTasks; b++ {
		p := OutputName(c.spec.Output.Filebase, b)
		files = append(files, p)
	}
	return files
}
