package mapreduce

import (
	"fmt"
	"time"
)

type Input struct {
	Mapper      string `json:"mapper"`
	FilePattern string `json:"file_pattern"`
	Format      string `json:"format"`
}

type Output struct {
	Filebase string `json:"filebase"`
	NumTasks int    `json:"num_tasks"`
	Format   string `json:"format"`
	Reducer  string `json:"reducer"`
	Combiner string `json:"combiner"`
}

type Specification struct {
	Inputs     []Input `json:"inputs"`
	Output     Output  `json:"output"`
	Machines   int     `json:"machines"`
	TaskPrefix string  `json:"task_prefix"`
}

type Result struct {
	Counters    map[string]int64
	Elapsed     time.Duration
	OutputFiles []string
}

func OutputName(base string, b int) string {
	return fmt.Sprintf("%s-%d", base, b)
}
