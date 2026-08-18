package mapreduce

import (
	"context"
	"encoding/gob"
)

type TaskStatus int

const (
	StatusPending TaskStatus = iota
	StatusRunning
	StatusDone
	StatusFailed
)

type TaskType int

const (
	MapType TaskType = iota
	ReduceType
	NoneType
)

// for rpc to work on these interfaces
func init() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
}

type Task interface {
	GetID() int
	Type() TaskType
	Execute(context.Context) error
}

type MapTask struct {
	ID int

	InputFile       string // file the mapper reads
	InputFormat     string // format name for reading InputFile
	NumReduce       int    // number of reduce buckets
	Mapper          string // registered mapper name
	Combiner        string // optional registered reducer used as combiner
	IntermediateDir string // directory for intermediate bucket files

	Format string
}

func (m MapTask) GetID() int {
	return m.ID
}

func (m MapTask) Type() TaskType {
	return MapType
}

func (m MapTask) Execute(ctx context.Context) error {
	// TODO: Implement the execute for Map Task
	return nil
}

type ReduceTask struct {
	ID              int
	Bucket          int    // reduce bucket index
	NumMap          int    // number of map tasks producing intermediate files
	Reducer         string // registered reducer name
	Combiner        string // registered combiner name
	OutputBase      string // output file base name (Output.FileBase)
	IntermediateDir string

	Format string
}

func (r ReduceTask) GetID() int {
	return r.ID
}

func (ReduceTask) Type() TaskType {
	return ReduceType
}

func (r ReduceTask) Execute(ctx context.Context) error {
	// TODO: Implement the execute for Reduce Task
	return nil
}
