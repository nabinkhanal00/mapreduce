package mapreduce

import (
	"bufio"
	"bytes"
	"os"
)

type KV struct{}

func (KV) Name() string {
	return "kv"
}

func (KV) Reader(path string) (RecordReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &kvReader{
		sc: bufio.NewScanner(f),
		f:  f,
	}, nil
}

type kvReader struct {
	sc  *bufio.Scanner
	f   *os.File
	rec Record
	err error
}

func (r *kvReader) Next() bool {
	if !r.sc.Scan() {
		r.err = r.sc.Err()
		return false
	}
	line := r.sc.Bytes()
	i := bytes.IndexAny(line, " \t")
	if i < 0 {
		r.rec.Key = append([]byte(nil), line[:i]...)
		r.rec.Value = nil
	} else {
		r.rec.Key = append([]byte(nil), line[:i]...)
		r.rec.Value = append([]byte(nil), line[i+1:]...)
	}
	return true
}

func (r *kvReader) Record() Record { return r.rec }
func (r *kvReader) Err() error     { return r.err }
func (r *kvReader) Close() error   { return r.f.Close() }

func (KV) Writer(path string) (RecordWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &lineWriter{
		bw: bufio.NewWriter(f),
		f:  f,
	}, nil
}

// for init() to be called
var _ Format = KV{}
