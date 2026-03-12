package pool

import "sync"

type Pool[T any] struct {
	p *sync.Pool
}

func New[T any](newFn func() T) *Pool[T] {
	return &Pool[T]{
		p: &sync.Pool{
			New: func() interface{} {
				return newFn()
			},
		},
	}
}

func (tp *Pool[T]) Get() T {
	return tp.p.Get().(T)
}

func (tp *Pool[T]) Put(t T) {
	tp.p.Put(t)
}
