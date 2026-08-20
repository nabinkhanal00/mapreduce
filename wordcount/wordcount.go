// Package wordcount provides a word count mapper and reducer that can
// be registered with the mapreduce framework. Both the master and the
// worker binaries must register the same set of names so that tasks can
// be resolved by name on either side.
package wordcount

import (
	"bytes"
	"context"
	"strconv"

	"github.com/nabinkhanal00/labs/mapreduce"
	"github.com/nabinkhanal00/labs/mapreduce/format"
)

// Register registers the word count mapper and reducer together with the
// built-in file formats under their lookup names.
func Register() {
	mapreduce.RegisterFormat("text", format.LineFormat{})
	mapreduce.RegisterFormat("kv", format.KVFormat{})
	mapreduce.RegisterFormat("textkv", format.TextKVFormat{})
	mapreduce.RegisterMapper("wc", mapreduce.MapperFunc(Mapper))
	mapreduce.RegisterReducer("wc", mapreduce.ReducerFunc(Reducer))
}

// Mapper emits one record (word, 1) for every whitespace-separated
// token in each input line. The input key is unused.
func Mapper(ctx context.Context, key []byte, ival mapreduce.RecordIterator, emit mapreduce.Emitter) error {
	for ival.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, word := range bytes.Fields(ival.Value().Value) {
			if err := emit.Emit(ctx, word, []byte("1")); err != nil {
				return err
			}
		}
	}
	return ival.Err()
}

// Reducer sums the per-key counts. The records of a partition are not
// guaranteed to be grouped by key, so counts are accumulated in a map
// before being emitted.
func Reducer(ctx context.Context, ival mapreduce.RecordIterator, emit mapreduce.Emitter) error {
	counts := make(map[string]int64)
	for ival.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec := ival.Value()
		n, err := strconv.ParseInt(string(rec.Value), 10, 64)
		if err != nil {
			return err
		}
		counts[string(rec.Key)] += n
	}
	if err := ival.Err(); err != nil {
		return err
	}
	for key, total := range counts {
		if err := emit.Emit(ctx, []byte(key), []byte(strconv.FormatInt(total, 10))); err != nil {
			return err
		}
	}
	return nil
}
