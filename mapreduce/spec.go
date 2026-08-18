package mapreduce

import (
	"fmt"
	"time"
)

type Input struct {
	Mapper      string
	FilePattern string
	Format      string
}

type Output struct {
	Filebase string
	NumTasks int
	Format   string
	Reducer  string
	Combiner string
}

type Specification struct {
	Inputs   []Input
	Output   Output
	Machines int
}

type Result struct {
	Counters    map[string]int64
	Elapsed     time.Duration
	OutputFiles []string
}

func OutputName(base string, b int) string {
	return fmt.Sprintf("%s-%d", base, b)
}
