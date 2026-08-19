package mapreduce

type SliceIterator[T any] struct {
	items []T
	index int
}

func NewSliceIterator[T any](items []T) *SliceIterator[T] {
	return &SliceIterator[T]{
		items: items,
		index: -1,
	}
}

func (it *SliceIterator[T]) Next() bool {
	it.index++
	return it.index < len(it.items)
}

func (it *SliceIterator[T]) Value() T {
	return it.items[it.index]
}

func (it *SliceIterator[T]) Err() error {
	return nil
}
