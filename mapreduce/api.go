// Package mapreduce contains code for map and reduce
package mapreduce

import "context"

type Iterator interface {
	Next() bool
	Value() bool
	Err() error
}

type Mapper interface {
	Map(ctx context.Context, key []byte, ival Iterator, emit Emitter) error
}

type Emitter interface {
	Emit(ctx context.Context, key []byte, val []byte) error
}

type Reducer interface {
	Reduce(ctx context.Context, key []byte, ival Iterator, emit Emitter) error
}
