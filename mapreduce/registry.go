package mapreduce

import (
	"fmt"
	"sync"
)

var (
	muMapper  sync.RWMutex
	muReducer sync.RWMutex
	muFormat  sync.RWMutex

	mappers  map[string]Mapper  = make(map[string]Mapper)
	reducers map[string]Reducer = make(map[string]Reducer)
	formats  map[string]Format  = make(map[string]Format)
)

func RegisterMapper(key string, m Mapper) {
	muMapper.Lock()
	defer muMapper.Unlock()
	if m == nil {
		panic("mapper cannot be nil")
	}
	if _, present := mappers[key]; present {
		panic("mapper is already registered: " + key)
	}
	mappers[key] = m
}

func RegisterReducer(key string, r Reducer) {
	muReducer.Lock()
	defer muReducer.Unlock()
	if r == nil {
		panic("ERROR: reducer cannot be nil")
	}
	if _, present := reducers[key]; present {
		panic("ERROR: reducer is already registered: " + key)
	}
	reducers[key] = r
}

func RegisterFormat(key string, f Format) {
	muFormat.Lock()
	defer muFormat.Unlock()
	if f == nil {
		panic("ERROR: format cannot be nil")
	}
	if _, present := formats[key]; present {
		panic("ERROR: format is already registered: " + key)
	}
	formats[key] = f
}

func lookupMapper(key string) (Mapper, error) {
	muMapper.RLock()
	defer muMapper.RUnlock()
	m, ok := mappers[key]
	if !ok {
		return nil, fmt.Errorf("mapreduce: no Mapper registered under name %q", key)
	}
	return m, nil
}

func lookupReducer(key string) (Reducer, error) {
	muReducer.RLock()
	defer muReducer.RUnlock()
	r, ok := reducers[key]
	if !ok {
		return nil, fmt.Errorf("mapreduce: no Reducer registered under name %q", key)
	}
	return r, nil
}

func lookupFormat(key string) (Format, error) {
	muFormat.RLock()
	defer muFormat.RUnlock()
	f, ok := formats[key]
	if !ok {
		return nil, fmt.Errorf("mapreduce: no Format registered under name %q", key)
	}
	return f, nil
}
