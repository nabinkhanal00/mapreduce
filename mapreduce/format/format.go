// Package format contains all possible formats
package format

import "io"

type Record struct {
	Key   []byte
	Value []byte
}

type Reader interface {
	Next() bool
	Value() Record
	Err() error
	Close() error
}

type Writer interface {
	Write(Record) error
	Close() error
}

type Format interface {
	Name() string

	Reader(reader io.Reader) (Reader, error)
	Writer(writer io.WriteCloser) (Writer, error)
}
