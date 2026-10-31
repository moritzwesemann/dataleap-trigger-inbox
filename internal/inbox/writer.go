package inbox

import (
	"context"

	"triggerinbox/internal/store"
)

const (
	writers  = 16
	maxBatch = 500
)

type pending struct {
	event store.NewEvent
	done  chan saved
}

type saved struct {
	isNew bool
	err   error
}

func (in *Inbox) Start(ctx context.Context) {
	for range writers {
		go in.write(ctx)
	}
}

func (in *Inbox) save(ctx context.Context, e store.NewEvent) (bool, error) {
	p := pending{event: e, done: make(chan saved, 1)}
	select {
	case in.queue <- p:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	select {
	case s := <-p.done:
		return s.isNew, s.err
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (in *Inbox) write(ctx context.Context) {
	for {
		var batch []pending
		select {
		case <-ctx.Done():
			return
		case p := <-in.queue:
			batch = append(batch, p)
		}
	fill:
		for len(batch) < maxBatch {
			select {
			case p := <-in.queue:
				batch = append(batch, p)
			default:
				break fill
			}
		}
		in.flush(ctx, batch)
	}
}

func (in *Inbox) flush(ctx context.Context, batch []pending) {
	events := make([]store.NewEvent, len(batch))
	for i, p := range batch {
		events[i] = p.event
	}
	saveCtx, cancel := context.WithTimeout(ctx, in.SaveTimeout)
	defer cancel()
	isNew, err := in.store.InsertBatch(saveCtx, events)
	for i, p := range batch {
		p.done <- saved{isNew: err == nil && isNew[i], err: err}
	}
}
