package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"triggerinbox/internal/agent"
	"triggerinbox/internal/store"
)

type Config struct {
	Workers      int
	Reservation  time.Duration
	Heartbeat    time.Duration
	PollInterval time.Duration
}

type Pool struct {
	store  *store.Store
	runner agent.Runner
	cfg    Config
	log    *slog.Logger
}

func New(s *store.Store, r agent.Runner, cfg Config, log *slog.Logger) *Pool {
	return &Pool{store: s, runner: r, cfg: cfg, log: log}
}

func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { p.releaseExpired(ctx) })
	for range p.cfg.Workers {
		wg.Go(func() { p.work(ctx) })
	}
	wg.Wait()
}

func (p *Pool) work(ctx context.Context) {
	cursor := ""
	for ctx.Err() == nil {
		l, err := p.store.Claim(ctx, cursor, p.cfg.Reservation)
		if err != nil && ctx.Err() == nil {
			p.log.Error("claim failed", "err", err)
		}
		if l == nil {
			pause(ctx, p.cfg.PollInterval)
			continue
		}
		cursor = l.AgentID
		p.process(ctx, l)
	}
}

func (p *Pool) process(ctx context.Context, l *store.Lease) {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost bool
	var hb sync.WaitGroup
	hb.Go(func() {
		if lost = p.heartbeat(runCtx, l); lost {
			cancel()
		}
	})
	err := p.runner.Run(runCtx, l.Event)
	cancel()
	hb.Wait()

	switch {
	case ctx.Err() != nil:
		return
	case lost:
		err = store.ErrLeaseLost
	case errors.Is(err, agent.ErrWorkerCrash):
		return
	case err == nil:
		err = p.store.Complete(ctx, l)
	default:
		p.log.Warn("run failed", "agent_id", l.AgentID, "event_id", l.EventID, "reason", err.Error())
		err = p.store.Fail(ctx, l, err.Error())
	}
	switch {
	case ctx.Err() != nil:
	case errors.Is(err, store.ErrLeaseLost):
		p.log.Warn("reservation lost, another worker owns the event", "agent_id", l.AgentID, "event_id", l.EventID)
	case err != nil:
		p.log.Error("recording result failed", "agent_id", l.AgentID, "event_id", l.EventID, "err", err)
	}
}

func (p *Pool) heartbeat(ctx context.Context, l *store.Lease) (lost bool) {
	t := time.NewTicker(p.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			err := p.store.Renew(ctx, l, p.cfg.Reservation)
			if errors.Is(err, store.ErrLeaseLost) {
				return true
			}
			if err != nil && ctx.Err() == nil {
				p.log.Error("renewing the reservation failed", "agent_id", l.AgentID, "event_id", l.EventID, "err", err)
			}
		}
	}
}

func (p *Pool) releaseExpired(ctx context.Context) {
	t := time.NewTicker(p.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := p.store.ReleaseExpired(ctx)
			if err != nil {
				if ctx.Err() == nil {
					p.log.Error("releasing expired reservations failed", "err", err)
				}
				continue
			}
			if n > 0 {
				p.log.Info("released expired reservations", "count", n)
			}
		}
	}
}

func pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
