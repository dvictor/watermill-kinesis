package closer

import (
	"context"
	"sync"
)

// A Closer allows you to close goroutines spawned by the owner via a derived cancellable context.
//
// Basic example:
//
//	var owner.closer = NewCloser()
//	func (o *owner) startWork(ctx context.Context) {
//		ctx = o.closer.Start(ctx)
//		go func() {
//			doWork(ctx)
//			s.closer.Done()
//		}
//	}
//	func (o *owner) Close() error {
//		return o.closer.Close()
//	}
type Closer interface {
	Start(ctx context.Context) context.Context
	Done()
	Close() error
}

func NewCloser() Closer {
	return &closer{
		cancel: make(chan struct{}),
	}
}

type closer struct {
	cancel chan struct{}
	wg     sync.WaitGroup
}

func (s *closer) Start(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	select {
	case <-s.cancel:
		cancel()
		return ctx
	default:
	}
	go func() {
		select {
		case <-s.cancel:
			cancel()
		case <-ctx.Done():
		}
	}()
	s.wg.Add(1)
	return ctx
}

func (s *closer) Done() {
	s.wg.Done()
}

func (s *closer) Close() error {
	close(s.cancel)
	s.wg.Wait()
	return nil
}
