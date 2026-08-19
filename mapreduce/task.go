package mapreduce

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"os"
	"slices"

	"github.com/nabinkhanal00/labs/mapreduce/format"
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
	GetID() string
	Type() TaskType
	Execute(context.Context) error
}

type MapTask struct {
	ID string

	FileSourceAddress string

	InputFile       string // file the mapper reads
	InputFormat     string // format name for reading InputFile
	NumReduce       int    // number of reduce buckets
	Mapper          string // registered mapper name
	Combiner        string // optional registered reducer used as combiner
	IntermediateDir string // directory for intermediate bucket files

	Format string
}

func (m MapTask) GetID() string {
	return m.ID
}

func (m MapTask) Type() TaskType {
	return MapType
}

func (m MapTask) Execute(ctx context.Context) error {
	conn, err := NewFileServerConnection(m.FileSourceAddress)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Println(err)
		}
	}()
	content, err := conn.GetFileContents(m.InputFile)
	if err != nil {
		return err
	}
	inFormat, err := lookupFormat(m.InputFormat)
	if err != nil {
		return err
	}
	outFormat, err := lookupFormat(m.Format)
	if err != nil {
		return err
	}

	mapper, err := lookupMapper(m.Mapper)
	if err != nil {
		return err
	}
	var combiner Reducer
	if m.Combiner != "" {
		combiner, err = lookupReducer(m.Combiner)
		if err != nil {
			return err
		}
	}

	inputBuffer := bytes.NewBuffer(content)
	reader, err := inFormat.Reader(inputBuffer)
	if err != nil {
		return err
	}

	buckets := make([][]format.Record, m.NumReduce)

	mapperEmitter := EmitterFunc(func(ctx context.Context, key []byte, value []byte) error {
		h := fnv.New32a()
		h.Write(key)
		idx := h.Sum32() % uint32(m.NumReduce)
		buckets[idx] = append(buckets[idx], format.Record{Key: key, Value: value})
		return nil
	})
	if err := mapper.Map(ctx, []byte(m.ID), reader, mapperEmitter); err != nil {
		return err
	}

	if m.Combiner != "" {
		for i, bucket := range buckets {
			records := []format.Record{}

			combinerEmitter := EmitterFunc(func(ctx context.Context, key []byte, value []byte) error {
				records = append(records, format.Record{Key: key, Value: value})
				return nil
			})
			recordIterator := NewSliceIterator(bucket)
			if err := combiner.Reduce(ctx, recordIterator, combinerEmitter); err != nil {
				return err
			}
			buckets[i] = records
		}
	}
	for i, bucket := range buckets {
		f, err := os.CreateTemp("", "*")
		if err != nil {
			return err
		}
		recordWriter, err := outFormat.Writer(f)
		if err != nil {
			return err
		}
		// closes the underlying file as well
		slices.SortFunc(bucket, func(a, b format.Record) int { return bytes.Compare(a.Key, b.Key) })
		for _, record := range bucket {
			_ = recordWriter.Write(record)
		}
		destFilename := fmt.Sprintf("%s/%s-%d", m.IntermediateDir, m.ID, i)
		_, _ = f.Seek(0, io.SeekStart)
		_ = conn.PutFileContents(f, destFilename)
		_ = os.Remove(f.Name())
		_ = recordWriter.Close()
	}

	return nil
}

type ReduceTask struct {
	ID              string
	Bucket          int    // reduce bucket index
	NumMap          int    // number of map tasks producing intermediate files
	Reducer         string // registered reducer name
	Combiner        string // registered combiner name
	OutputBase      string // output file base name (Output.FileBase)
	IntermediateDir string

	Format            string
	FileSourceAddress string
	MapFilePrefix     string
}

func (r ReduceTask) GetID() string {
	return r.ID
}

func (ReduceTask) Type() TaskType {
	return ReduceType
}

func (r ReduceTask) Execute(ctx context.Context) error {
	if r.NumMap <= 0 {
		return fmt.Errorf("mapreduce: NumMap must be greater than zero")
	}

	conn, err := NewFileServerConnection(r.FileSourceAddress)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("mapreduce: closing file connection: %v", err)
		}
	}()

	reducer, err := lookupReducer(r.Reducer)
	if err != nil {
		return err
	}

	formatter, err := lookupFormat(r.Format)
	if err != nil {
		return err
	}

	// Fetch all intermediate files belonging to this
	// reduce bucket.
	var records []format.Record

	for mapID := 0; mapID < r.NumMap; mapID++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		filename := fmt.Sprintf(
			"%s/%s-%d-%d",
			r.IntermediateDir,
			r.MapFilePrefix,
			mapID,
			r.Bucket,
		)

		content, err := conn.GetFileContents(filename)
		if err != nil {
			return fmt.Errorf(
				"mapreduce: get intermediate file %s: %w",
				filename,
				err,
			)
		}

		reader, err := formatter.Reader(bytes.NewReader(content))
		if err != nil {
			return fmt.Errorf(
				"mapreduce: create reader for %s: %w",
				filename,
				err,
			)
		}

		for reader.Next() {
			records = append(records, reader.Value())
		}

		if err := reader.Err(); err != nil {
			return fmt.Errorf(
				"mapreduce: read intermediate file %s: %w",
				filename,
				err,
			)
		}
	}

	// The reducer receives the entire reduce partition and
	// is responsible for grouping records by key.
	input := NewSliceIterator(records)

	// Collect reducer output.
	var output []format.Record

	emitter := EmitterFunc(
		func(
			ctx context.Context,
			key []byte,
			value []byte,
		) error {
			output = append(output, format.Record{
				Key:   key,
				Value: value,
			})

			return nil
		},
	)

	if err := reducer.Reduce(
		ctx,
		input,
		emitter,
	); err != nil {
		return fmt.Errorf(
			"mapreduce: reduce task %s: %w",
			r.ID,
			err,
		)
	}

	// Sort reducer output by key.
	slices.SortFunc(output, func(a, b format.Record) int {
		return bytes.Compare(a.Key, b.Key)
	})

	// Create local output file.
	f, err := os.CreateTemp("", "mapreduce-reduce-*")
	if err != nil {
		return fmt.Errorf("mapreduce: create output file: %w", err)
	}

	tempName := f.Name()

	writer, err := formatter.Writer(f)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(tempName)

		return fmt.Errorf(
			"mapreduce: create output writer: %w",
			err,
		)
	}

	for _, record := range output {
		if err := writer.Write(
			record,
		); err != nil {
			_ = writer.Close()
			_ = f.Close()
			_ = os.Remove(tempName)

			return fmt.Errorf(
				"mapreduce: write output: %w",
				err,
			)
		}
	}

	// Flush/close the format writer before uploading.
	if err := writer.Close(); err != nil {
		_ = f.Close()
		_ = os.Remove(tempName)

		return fmt.Errorf(
			"mapreduce: close output writer: %w",
			err,
		)
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		_ = os.Remove(tempName)

		return fmt.Errorf(
			"mapreduce: seek output file: %w",
			err,
		)
	}

	// Upload final output.
	outputFilename := r.OutputBase

	if err := conn.PutFileContents(
		f,
		outputFilename,
	); err != nil {
		_ = f.Close()
		_ = os.Remove(tempName)

		return fmt.Errorf(
			"mapreduce: upload output %s: %w",
			outputFilename,
			err,
		)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(tempName)
		return fmt.Errorf(
			"mapreduce: close output file: %w",
			err,
		)
	}

	if err := os.Remove(tempName); err != nil {
		return fmt.Errorf(
			"mapreduce: remove temporary output: %w",
			err,
		)
	}

	return nil
}
