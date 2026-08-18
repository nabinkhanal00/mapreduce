package mapreduce

import (
	"fmt"
	"net"
	"net/rpc"
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
	phaseMap phase = iota
	phaseReduce
	phaseDone
)

type Coordinator struct {
	mu   sync.Mutex
	spec Specification

	intermediateDir string
	addr            string

	mapTasks    []*taskState
	reduceTasks []*taskState

	phase    phase
	started  time.Time
	done     chan struct{}
	doneOnce sync.Once

	taskTimeout     time.Duration
	waitHealthCheck time.Duration

	result Result
}

func NewCoordinator(spec Specification, intermediateDir string, timeout time.Duration) (*Coordinator, error) {
	if spec.Output.NumTasks < 1 {
		return nil, fmt.Errorf("mapreduce: Output.NumTasks must be >= 1, got %d", spec.Output.NumTasks)
	}
	if spec.Output.Filebase == "" {
		return nil, fmt.Errorf("mapreduce: Output.Filebase must not be empty")
	}
	if spec.Output.Reducer == "" {
		return nil, fmt.Errorf("mapreduce: Output.Reducer must not be empty")
	}

	c := &Coordinator{
		spec:            spec,
		intermediateDir: intermediateDir,
		done:            make(chan struct{}),
		started:         time.Now(),
		taskTimeout:     timeout,
		waitHealthCheck: 30 * time.Second,
	}
	if err := c.buildTasks(); err != nil {
		return nil, err
	}
	return c, nil
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
		c.mapTasks = append(c.mapTasks, &taskState{
			task: &MapTask{
				ID: i,

				InputFile:   in.path,
				InputFormat: in.format,
				NumReduce:   c.spec.Output.NumTasks,
				Mapper:      in.mapper,
				Combiner:    c.spec.Output.Combiner,

				IntermediateDir: c.intermediateDir,
				Format:          outFormat,
			},
			status: StatusPending,
		})
	}
	for i := range c.spec.Output.NumTasks {
		c.reduceTasks = append(c.reduceTasks, &taskState{
			task: &ReduceTask{
				ID:       i,
				Bucket:   i,
				NumMap:   len(c.mapTasks),
				Reducer:  c.spec.Output.Reducer,
				Combiner: c.spec.Output.Combiner,

				Format:          outFormat,
				IntermediateDir: c.intermediateDir,
			},
			status: StatusPending,
		})
	}
	return nil
}

func (c *Coordinator) Serve() (net.Listener, error) {
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}
	server := rpc.NewServer()
	if err := server.RegisterName("Coordinator", c); err != nil {
		_ = listener.Close()
		return nil, err
	}
	go accept(server, listener)
	c.addr = listener.Addr().String()
	return listener, nil
}

func accept(server *rpc.Server, listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
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
		return nil
	}
	if args.Err != "" {
		ts.status = StatusPending
		ts.hostid = ""
	}
	ts.status = StatusDone

	switch c.phase {
	case phaseMap:
		if c.allDoneLocked(c.mapTasks) {
			c.startReduceLocked()
		}
	case phaseReduce:
		if c.allDoneLocked(c.reduceTasks) {
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
	}
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
			return nil
		}
		if !c.allDoneLocked(c.mapTasks) {
			return nil
		}
		c.startReduceLocked()
		if ts := c.pickPendingLocked(c.reduceTasks); ts != nil {
			ts.hostid = args.HostID
			reply.Task = ts.task
			return nil
		}
		return nil
	case phaseReduce:
		if ts := c.pickPendingLocked(c.reduceTasks); ts != nil {
			ts.hostid = args.HostID
			reply.Task = ts.task
			return nil
		}
		if !c.allDoneLocked(c.mapTasks) {
			return nil
		}
		c.finishLocked()
		reply.Done = true
		return nil
	default:
		reply.Done = true
		return nil
	}
}

func (c *Coordinator) readExpiredLocked() {
	now := time.Now()
	for _, ts := range c.mapTasks {
		if ts.status == StatusRunning && (now.After(ts.deadline) || now.Sub(ts.healthCheck) > c.waitHealthCheck) {
			ts.status = StatusPending
			ts.hostid = ""
		}
	}
	for _, ts := range c.reduceTasks {
		if ts.status == StatusRunning && (now.After(ts.deadline) || now.Sub(ts.healthCheck) > c.waitHealthCheck) {
			ts.status = StatusPending
			ts.hostid = ""
		}
	}
}

func (c *Coordinator) pickPendingLocked(tasks []*taskState) *taskState {
	for _, ts := range tasks {
		if ts.status == StatusPending {
			ts.status = StatusRunning
			ts.deadline = time.Now().Add(c.taskTimeout)
			ts.attempts++
			return ts
		}
	}
	return nil
}

func (c *Coordinator) allDoneLocked(tasks []*taskState) bool {
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
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *Coordinator) countDoneLocked(tasks []*taskState) int64 {
	n := 0
	for _, ts := range tasks {
		if ts.status == StatusDone {
			n++
		}
	}
	return int64(n)
}

func (c *Coordinator) findTaskLocked(id int, tp TaskType) *taskState {
	if tp == MapType {
		if id < 0 || id >= len(c.mapTasks) {
			return nil
		}
		return c.mapTasks[id]
	}
	if id < 0 || id >= len(c.reduceTasks) {
		return nil
	}
	return c.reduceTasks[id]
}

func (c *Coordinator) outputFiles() []string {
	var files []string
	for b := 0; b < c.spec.Output.NumTasks; b++ {
		p := OutputName(c.spec.Output.Filebase, b)
		files = append(files, p)
	}
	return files
}
