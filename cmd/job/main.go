package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nabinkhanal00/labs/mapreduce"
)

type submitRequest struct {
	Spec            mapreduce.Specification `json:"spec"`
	IntermediateDir string                  `json:"intermediate_dir"`
}

type jobInfo struct {
	ID          int              `json:"id"`
	Phase       string           `json:"phase"`
	Started     time.Time        `json:"started"`
	Finished    time.Time        `json:"finished"`
	Elapsed     time.Duration    `json:"elapsed"`
	Counters    map[string]int64 `json:"counters"`
	OutputFiles []string         `json:"output_files"`
	Error       string           `json:"error,omitempty"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "submit":
		submit(os.Args[2:])
	case "jobs":
		jobs(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: job <command> [flags]

commands:
  submit   submit a job specification to the master
  jobs     list jobs recorded by the master

run 'job <command> -h' for command-specific flags`)
	os.Exit(2)
}

func submit(args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	master := fs.String("master", "http://127.0.0.1:8080", "master HTTP API address")
	specPath := fs.String("spec", "-", "path to spec JSON file ('-' for stdin)")
	wait := fs.Bool("wait", false, "block until the job finishes and print its result")
	poll := fs.Duration("poll", time.Second, "status poll interval when -wait is set")
	_ = fs.Parse(args)

	req, err := readSpec(*specPath)
	if err != nil {
		fatal("read spec: %v", err)
	}
	body, err := json.Marshal(req)
	if err != nil {
		fatal("encode spec: %v", err)
	}
	resp, err := http.Post(strings.TrimRight(*master, "/")+"/api/v1/submit", "application/json", bytes.NewReader(body))
	if err != nil {
		fatal("submit: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		fatal("submit failed (%s): %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var out struct {
		JobID int `json:"job_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		fatal("decode response: %v", err)
	}
	fmt.Printf("submitted job id %d\n", out.JobID)
	if !*wait {
		return
	}
	for {
		info, err := fetchJob(*master, out.JobID)
		if err != nil {
			fatal("status: %v", err)
		}
		if info.Phase == "done" {
			printJob(info)
			return
		}
		time.Sleep(*poll)
	}
}

func jobs(args []string) {
	fs := flag.NewFlagSet("jobs", flag.ExitOnError)
	master := fs.String("master", "http://127.0.0.1:8080", "master HTTP API address")
	id := fs.Int("id", 0, "print only the job with this id")
	_ = fs.Parse(args)

	infos, err := fetchJobs(*master)
	if err != nil {
		fatal("list jobs: %v", err)
	}
	for _, info := range infos {
		if *id != 0 && info.ID != *id {
			continue
		}
		printJob(&info)
	}
}

func printJob(j *jobInfo) {
	fmt.Printf("job %d: phase=%s elapsed=%s counters=%v\n", j.ID, j.Phase, j.Elapsed, j.Counters)
	if len(j.OutputFiles) > 0 {
		fmt.Printf("  output files:\n")
		for _, f := range j.OutputFiles {
			fmt.Printf("    %s\n", f)
		}
	}
	if j.Error != "" {
		fmt.Printf("  error: %s\n", j.Error)
	}
}

func fetchJobs(master string) ([]jobInfo, error) {
	resp, err := http.Get(strings.TrimRight(master, "/") + "/api/v1/jobs")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	var out struct {
		Jobs []jobInfo `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

func fetchJob(master string, id int) (*jobInfo, error) {
	infos, err := fetchJobs(master)
	if err != nil {
		return nil, err
	}
	for i := range infos {
		if infos[i].ID == id {
			return &infos[i], nil
		}
	}
	return nil, fmt.Errorf("job %d not found", id)
}

// readSpec reads a submitRequest from a JSON file or stdin.
func readSpec(path string) (*submitRequest, error) {
	var r io.Reader
	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		r = f
	}
	var req submitRequest
	if err := json.NewDecoder(r).Decode(&req); err != nil {
		return nil, err
	}
	if len(req.Spec.Inputs) == 0 {
		return nil, fmt.Errorf("spec has no inputs")
	}
	return &req, nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "job: "+format+"\n", args...)
	os.Exit(1)
}
