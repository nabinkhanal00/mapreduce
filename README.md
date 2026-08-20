# MapReduce

A distributed MapReduce framework written in Go. It runs a long-lived
coordinator (master), any number of worker daemons, and a standalone file
server. Jobs are described as JSON specifications and submitted through a
small HTTP API or the bundled `job` CLI client. Workers and the coordinator
can run on different machines with no shared filesystem; all input,
intermediate, and output data moves through the file server.

## Architecture

```
                 job spec (JSON)
                         |
                         v
  +----------------------------------------------+
  |  cmd/master   (master daemon)                |
  |  - Coordinator RPC listener (net/rpc)        |
  |  - HTTP job API      /api/v1/submit, /jobs   |
  +----------------------------------------------+
        ^  GetTask / ReportTask / HealthCheck (net/rpc)
        |                   |
        |                   |
  +-----+-------+   +-------+------+       +------------------+
  | cmd/worker  |   | cmd/worker   |  ...  | cmd/fileserver   |
  | (node)      |   | (node)       |       | (shared storage) |
  +-------------+   +--------------+       +------------------+
        |                   |                    ^
        +-------- fetch input / write intermediate & output files -+
```

- **Master (`cmd/master`)** — hosts the `Coordinator`, which is long-lived.
  It accepts one job at a time, tracks map/reduce task state, hands tasks to
  polling workers, and requeues tasks whose workers crash or stall. It also
  serves the HTTP job API and keeps a job history.
- **Workers (`cmd/worker`)** — continuously poll the coordinator for tasks.
  They never exit on their own: they reconnect with backoff after
  coordinator restarts and run any number of jobs over time. Each worker
  registers a set of mappers, reducers, and formats by name.
- **File server (`cmd/fileserver`)** — a minimal TCP server that stores and
  serves raw files by absolute path. Every machine can read and write files
  through it, so nodes do not need a shared disk.
- **CLI client (`cmd/job`)** — submits job specs (from a file or stdin) and
  lists recorded jobs.

## Job lifecycle

1. A spec is submitted with `SubmitJob`. The coordinator validates it, globs
   the input `file_pattern`s against the master's filesystem, and builds one
   map task per matched input plus `NumTasks` reduce tasks.
2. The coordinator enters the **map** phase. Workers polling `GetTask` are
   handed pending map tasks.
3. Each map task fetches its input file from the file server, runs the named
   mapper, and partitions emitted records across `NumTasks` buckets by FNV-1a
   hash of the key. An optional combiner (any registered reducer) can
   pre-aggregate each bucket. Bucket files are written to
   `intermediate_dir` on the file server.
4. When all maps complete the coordinator switches to the **reduce** phase.
   Each reduce task fetches all intermediate files for its bucket, runs the
   named reducer, sorts output by key, and uploads its result to
   `output.filebase-<bucket>`.
5. When all reduces complete the coordinator enters the **done** phase and
   returns to **idle**, ready for the next job. Workers keep polling through
   idle/done so the listener never goes away.

### Failure handling

- Every assigned task has a deadline. Workers send periodic health checks
  while running a task; a worker that stops reporting or blows the deadline
  causes the task to be requeued for another worker.
- A worker that reports a task failure has that task requeued.
- Workers reconnect automatically after the master restarts; the next
  submitted job just runs on the (new) coordinator.

## Building

Requires Go 1.26+.

```sh
go build ./...
go vet ./...
```

Build all binaries:

```sh
go build -o bin/master     ./cmd/master
go build -o bin/worker     ./cmd/worker
go build -o bin/fileserver ./cmd/fileserver
go build -o bin/job        ./cmd/job
```

To run workers on Linux ARM machines from macOS, cross-compile with:

```sh
GOOS=linux GOARCH=arm64 go build -o bin/worker-linux-arm64 ./cmd/worker
```

## Quick start

Create some input files on the machine that will run the file server:

```sh
mkdir -p in out
printf 'the quick brown fox\njumps over the lazy dog\n' > in/a.txt
printf 'the fox and the dog\nthe quick brown fox again\n' > in/b.txt
```

Start the file server (listen on all interfaces so remote workers can reach
it):

```sh
./bin/fileserver -addr 0.0.0.0:59000
```

Start the master daemon:

```sh
./bin/master -fs 127.0.0.1:59000 -coordinator 0.0.0.0:39000 -http 0.0.0.0:8080
```

Start one or more workers (here on the same machine; on other machines point
`-coordinator` at the master's address):

```sh
./bin/worker -coordinator 127.0.0.1:39000
```

Submit a word-count job with the bundled `job` client:

```sh
./bin/job submit -master http://127.0.0.1:8080 -spec job.json -wait
```

`job.json`:

```json
{
  "intermediate_dir": "intermediate",
  "spec": {
    "inputs": [
      { "file_pattern": "in/*.txt", "format": "text", "mapper": "wc" }
    ],
    "output": {
      "filebase": "out/result",
      "num_tasks": 2,
      "format": "textkv",
      "reducer": "wc"
    },
    "machines": 2,
    "task_prefix": "mr"
  }
}
```

Result:

```
submitted job id 1
job 1: phase=done elapsed=1.4s counters=map[map_tasks:2 map_tasks_completed:2 reduce_tasks:2 reduce_tasks_completed:2]
  output files:
    out/result-0
    out/result-1
```

The spec can also be piped via stdin (`-spec -`) or posted directly:

```sh
curl -s -X POST http://127.0.0.1:8080/api/v1/submit -d @job.json
```

## Job specification

| Field | Type | Description |
| --- | --- | --- |
| `intermediate_dir` | string | Directory (on the file server) for map output. Defaults to `intermediate`. |
| `spec.inputs[]` | array | Input sources. |
| `input.file_pattern` | string | Glob, resolved against the **master's** filesystem. |
| `input.format` | string | Input format name (`text`, `kv`, `textkv`). Defaults to `text`. |
| `input.mapper` | string | Name of a registered mapper. Required. |
| `spec.output.filebase` | string | Base path for reduce output; files are written as `<filebase>-<n>`. Required. |
| `spec.output.num_tasks` | int | Number of reduce tasks / output files. Must be >= 1. |
| `spec.output.format` | string | Output format name. Defaults to `kv`. |
| `spec.output.reducer` | string | Name of a registered reducer. Required. |
| `spec.output.combiner` | string | Optional registered reducer run per map bucket. |
| `spec.machines` | int | Expected worker count (informational). |
| `spec.task_prefix` | string | Prefix for generated task IDs (e.g. `mr` -> `mr-map-0`). |

The submit endpoint returns `202` with `{"job_id": N}`. Requests that fail
validation (unknown mapper/reducer/format, empty glob matches, bad
`file_pattern`, or a job already running) return `400` with the reason.

## HTTP API

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/api/v1/submit` | Submit a job spec (body is the JSON above). Returns `{"job_id": N}`. |
| `GET` | `/api/v1/jobs` | List jobs recorded by this master (`?id=N` via the `job` client). |

`GET /api/v1/jobs` returns each job's phase, timestamps, elapsed time,
counters, output files, and the submitted spec.

## Built-in formats

Registered by `wordcount.Register()` on both the master and worker binaries:

| Name | Reader | Writer |
| --- | --- | --- |
| `text` | one record per line (key is nil, value is the line) | value followed by a newline |
| `kv` | binary: uvarint-length-prefixed key and value | binary records |
| `textkv` | `key<TAB>value` per line (no tab -> empty value) | `key<TAB>value\n` |

## Writing a mapper, reducer, and format

Both the master and the workers must register the **same names** so tasks can
be resolved on either side.

```go
mapreduce.RegisterMapper("wc", mapreduce.MapperFunc(func(ctx context.Context, key []byte, ival mapreduce.RecordIterator, emit mapreduce.Emitter) error {
    for ival.Next() {
        for _, w := range bytes.Fields(ival.Value().Value) {
            if err := emit.Emit(ctx, w, []byte("1")); err != nil {
                return err
            }
        }
    }
    return ival.Err()
}))

mapreduce.RegisterReducer("wc", mapreduce.ReducerFunc(func(ctx context.Context, ival mapreduce.RecordIterator, emit mapreduce.Emitter) error {
    // accumulate counts per key, then emit.Emit(key, total)
    return nil
}))

mapreduce.RegisterFormat("myformat", myFormat{}) // implements format.Format
```

`Mapper`, `Reducer`, and `Emitter` are function-style interfaces
(`mapreduce/api.go`); records are `format.Record{Key, Value []byte}`.
The reference implementation is in `wordcount/wordcount.go`.

## Remote deployment notes

- The file server, master, and the machines holding input/output files are
  decoupled from workers: only the worker needs network access to the
  coordinator RPC and the file server.
- On the master, globs are resolved locally, and the resulting absolute
  paths are what workers request from the file server.
- To run a worker as a detached daemon over ssh without hanging the session,
  background `setsid` and redirect all standard streams:

```sh
#!/bin/bash
cd "$(dirname "$0")"
setsid ./worker "$@" > worker.log 2>&1 </dev/null &
disown 2>/dev/null || true
```

then `ssh host 'start-worker.sh -coordinator 192.168.1.65:39000'`.

## Repository layout

```
cmd/fileserver/    standalone file server
cmd/master/        master daemon (coordinator RPC + HTTP job API)
cmd/worker/        worker daemon
cmd/job/           job submission/status client
cmd/mr/            placeholder
mapreduce/         framework library
  api.go           Mapper/Reducer/Emitter interfaces
  master.go        Coordinator, phases, task scheduling, failure handling
  worker.go        resilient worker poll loop, health checks
  spec.go          Specification schema
  task.go          MapTask / ReduceTask execution
  file.go          file server wire protocol
  registry.go      named mapper/reducer/format registration
  format/          Record/Reader/Writer + text, kv, textkv formats
wordcount/         example mapper + reducer ("wc")
```