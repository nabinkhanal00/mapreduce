package format

import (
	"bufio"
	"errors"
	"io"
)

type LineFormat struct{}

func (LineFormat) Name() string {
	return "text"
}

func (LineFormat) Reader(r io.Reader) (Reader, error) {
	if r == nil {
		return nil, errors.New("nil reader")
	}

	return &lineReader{
		scanner: bufio.NewScanner(r),
	}, nil
}

func (LineFormat) Writer(w io.WriteCloser) (Writer, error) {
	if w == nil {
		return nil, errors.New("nil writer")
	}

	return &lineWriter{
		writer: bufio.NewWriter(w),
		closer: w,
	}, nil
}

type lineReader struct {
	scanner *bufio.Scanner
	value   string
	err     error
}

func (r *lineReader) Next() bool {
	if r.scanner.Scan() {
		r.value = r.scanner.Text()
		return true
	}

	r.err = r.scanner.Err()
	return false
}

func (r *lineReader) Value() Record {
	return Record{nil, []byte(r.value)}
}

func (r *lineReader) Err() error {
	return r.err
}

func (r *lineReader) Close() error {
	// There is nothing to close because the underlying
	// io.Reader is not necessarily an io.Closer.
	return nil
}

type lineWriter struct {
	writer *bufio.Writer
	closer io.Closer
}

func (w *lineWriter) Write(value Record) error {
	_, err := w.writer.Write(value.Value)
	if err != nil {
		return err
	}

	return w.writer.WriteByte('\n')
}

func (w *lineWriter) Close() error {
	flushErr := w.writer.Flush()
	closeErr := w.closer.Close()

	if flushErr != nil {
		return flushErr
	}

	return closeErr
}
