// Package mapreduce contains code for map and reduce
package mapreduce

import (
	"context"

	"github.com/nabinkhanal00/labs/mapreduce/format"
)

type RecordIterator interface {
	Next() bool
	Value() format.Record
	Err() error
}

type Mapper interface {
	Map(ctx context.Context, key []byte, ival RecordIterator, emit Emitter) error
}
type MapperFunc func(ctx context.Context, key []byte, ival RecordIterator, emit Emitter) error

func (f MapperFunc) Map(ctx context.Context, key []byte, ival RecordIterator, emit Emitter) error {
	return f(ctx, key, ival, emit)
}

type Emitter interface {
	Emit(ctx context.Context, key []byte, val []byte) error
}

type EmitterFunc func(ctx context.Context, key []byte, val []byte) error

func (f EmitterFunc) Emit(ctx context.Context, key []byte, val []byte) error {
	return f(ctx, key, val)
}

type Reducer interface {
	Reduce(ctx context.Context, ival RecordIterator, emit Emitter) error
}

type ReducerFunc func(ctx context.Context, ival RecordIterator, emit Emitter) error

func (f ReducerFunc) Reduce(ctx context.Context, ival RecordIterator, emit Emitter) error {
	return f(ctx, ival, emit)
}
