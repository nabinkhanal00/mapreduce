package mapreduce

type Record struct {
	Key   []byte
	Value []byte
}

type RecordReader interface {
	Next() bool
	Record() Record
	Err() error
	Close() error
}

type RecordWriter interface {
	Write(Record) error
	Close() error
}

type Format interface {
	Name() string

	Reader(path string) (RecordReader, error)
	Writer(path string) (RecordWriter, error)
}
