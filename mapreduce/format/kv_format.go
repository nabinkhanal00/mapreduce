package format

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
)

type KVFormat struct{}

func (KVFormat) Name() string {
	return "kv"
}

func (KVFormat) Reader(r io.Reader) (Reader, error) {
	if r == nil {
		return nil, errors.New("nil reader")
	}

	return &kvReader{
		reader: bufio.NewReader(r),
	}, nil
}

func (KVFormat) Writer(w io.WriteCloser) (Writer, error) {
	if w == nil {
		return nil, errors.New("nil writer")
	}

	return &kvWriter{
		writer: bufio.NewWriter(w),
		closer: w,
	}, nil
}

type kvReader struct {
	reader *bufio.Reader
	value  Record
	err    error
}

func (r *kvReader) Next() bool {
	keyLen, err := binary.ReadUvarint(r.reader)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false
		}

		r.err = err
		return false
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r.reader, key); err != nil {
		r.err = err
		return false
	}

	valueLen, err := binary.ReadUvarint(r.reader)
	if err != nil {
		r.err = err
		return false
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(r.reader, value); err != nil {
		r.err = err
		return false
	}

	r.value = Record{
		Key:   key,
		Value: value,
	}

	return true
}

func (r *kvReader) Value() Record {
	return r.value
}

func (r *kvReader) Err() error {
	return r.err
}

func (r *kvReader) Close() error {
	return nil
}

type kvWriter struct {
	writer *bufio.Writer
	closer io.Closer
}

func (w *kvWriter) Write(value Record) error {
	if err := writeUvarint(w.writer, uint64(len(value.Key))); err != nil {
		return err
	}

	if _, err := w.writer.Write(value.Key); err != nil {
		return err
	}

	if err := writeUvarint(w.writer, uint64(len(value.Value))); err != nil {
		return err
	}

	if _, err := w.writer.Write(value.Value); err != nil {
		return err
	}

	return nil
}

func (w *kvWriter) Close() error {
	flushErr := w.writer.Flush()
	closeErr := w.closer.Close()

	if flushErr != nil {
		return flushErr
	}

	return closeErr
}

func writeUvarint(w io.Writer, n uint64) error {
	var buf [binary.MaxVarintLen64]byte

	length := binary.PutUvarint(buf[:], n)

	_, err := w.Write(buf[:length])
	return err
}
