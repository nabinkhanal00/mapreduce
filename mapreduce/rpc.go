package mapreduce

type GetTaskArgs struct {
	HostID string
}

type GetTaskReply struct {
	Task Task
	Done bool
}

type ReportArgs struct {
	TaskID string
	HostID string
	Type   TaskType
	Err    string
}
type ReportReply struct{}

type HealthCheckArgs struct {
	HostID string
	TaskID string
	Type   TaskType
}
type HealthCheckReply struct {
	Stop bool
}
