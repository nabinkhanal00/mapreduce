package format

import (
	"bufio"
	"errors"
	"io"
)

type TextKVFormat struct{}

func (TextKVFormat) Name() string {
	return "text"
}

func (TextKVFormat) Reader(r io.Reader) (Reader, error) {
	if r == nil {
		return nil, errors.New("nil reader")
	}

	return &textkvReader{
		scanner: bufio.NewScanner(r),
	}, nil
}

func (TextKVFormat) Writer(w io.WriteCloser) (Writer, error) {
	if w == nil {
		return nil, errors.New("nil writer")
	}

	return &textkvwriter{
		writer: bufio.NewWriter(w),
		closer: w,
	}, nil
}

type textkvReader struct {
	scanner *bufio.Scanner
	value   Record
	err     error
}

func (r *textkvReader) Next() bool {
	if !r.scanner.Scan() {
		r.err = r.scanner.Err()
		return false
	}

	line := r.scanner.Bytes()

	// Split at the first tab.
	for i, b := range line {
		if b == '\t' {
			r.value = Record{
				Key:   append([]byte(nil), line[:i]...),
				Value: append([]byte(nil), line[i+1:]...),
			}
			return true
		}
	}

	// A line without a tab is treated as a key with an empty value.
	r.value = Record{
		Key:   append([]byte(nil), line...),
		Value: nil,
	}

	return true
}

func (r *textkvReader) Value() Record {
	return r.value
}

func (r *textkvReader) Err() error {
	return r.err
}

func (r *textkvReader) Close() error {
	return nil
}

type textkvwriter struct {
	writer *bufio.Writer
	closer io.Closer
}

func (w *textkvwriter) Write(value Record) error {
	if _, err := w.writer.Write(value.Key); err != nil {
		return err
	}

	if err := w.writer.WriteByte('\t'); err != nil {
		return err
	}

	if _, err := w.writer.Write(value.Value); err != nil {
		return err
	}

	return w.writer.WriteByte('\n')
}

func (w *textkvwriter) Close() error {
	flushErr := w.writer.Flush()
	closeErr := w.closer.Close()

	if flushErr != nil {
		return flushErr
	}

	return closeErr
}
