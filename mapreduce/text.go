package mapreduce

import (
	"bufio"
	"fmt"
	"os"
)

type Text struct{}

func (Text) Name() string {
	return "text"
}

func init() {
	RegisterFormat("text", Text{})
}

func (Text) Reader(path string) (RecordReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &lineReader{
		sc: bufio.NewScanner(f),
		f:  f,
	}, nil
}

type lineReader struct {
	sc   *bufio.Scanner
	f    *os.File
	line int64
	rec  Record
	err  error
}

func (r *lineReader) Next() bool {
	if !r.sc.Scan() {
		r.err = r.sc.Err()
		return false
	}

	r.line++
	r.rec.Key = fmt.Appendf(nil, "%d", r.line)
	r.rec.Value = append([]byte(nil), r.sc.Bytes()...)
	return true
}

func (r *lineReader) Record() Record { return r.rec }
func (r *lineReader) Err() error     { return r.err }
func (r *lineReader) Close() error   { return r.f.Close() }

func (Text) Writer(path string) (RecordWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &lineWriter{
		bw: bufio.NewWriter(f),
		f:  f,
	}, nil
}

type lineWriter struct {
	bw *bufio.Writer
	f  *os.File
}

func (w *lineWriter) Write(rec Record) error {
	_, err := w.bw.Write(fmt.Append(rec.Key, ' ', rec.Value))
	return err
}

func (w *lineWriter) Close() error {
	if err := w.bw.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// for init() to be called
var _ Format = Text{}
