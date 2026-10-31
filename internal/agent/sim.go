package agent

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"triggerinbox/internal/store"
)

var ErrWorkerCrash = errors.New("simulated worker crash")

var errAgent = errors.New("simulated agent error")

type Sim struct {
	MinDuration time.Duration
	MaxDuration time.Duration
	FailRate    float64
	CrashRate   float64
	Effects     *Effects
}

func (s *Sim) Run(ctx context.Context, e store.Event) error {
	s.Effects.start(e)
	defer s.Effects.stop(e)

	d := s.MinDuration
	if s.MaxDuration > s.MinDuration {
		d += rand.N(s.MaxDuration - s.MinDuration)
	}
	switch roll := rand.Float64(); {
	case roll < s.FailRate:
		if err := sleep(ctx, d/2); err != nil {
			return err
		}
		return errAgent
	case roll < s.FailRate+s.CrashRate:
		if err := sleep(ctx, d/2); err != nil {
			return err
		}
		if rand.IntN(2) == 0 {
			s.Effects.record(e)
		}
		return ErrWorkerCrash
	}
	if err := sleep(ctx, d); err != nil {
		return err
	}
	s.Effects.record(e)
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type key struct{ agentID, eventID string }

type Effects struct {
	mu         sync.Mutex
	count      map[key]int
	inFlight   map[key]int
	running    int
	maxRunning int
	overlaps   int
}

func NewEffects() *Effects {
	return &Effects{
		count:    map[key]int{},
		inFlight: map[key]int{},
	}
}

func (f *Effects) start(e store.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key{e.AgentID, e.EventID}
	f.running++
	f.maxRunning = max(f.maxRunning, f.running)
	if f.inFlight[k] > 0 {
		f.overlaps++
	}
	f.inFlight[k]++
}

func (f *Effects) stop(e store.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key{e.AgentID, e.EventID}
	f.running--
	if f.inFlight[k]--; f.inFlight[k] == 0 {
		delete(f.inFlight, k)
	}
}

func (f *Effects) record(e store.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key{e.AgentID, e.EventID}
	f.count[k]++
}

type Summary struct {
	Overlaps   int
	MaxRunning int
	Count      map[[2]string]int
}

func (f *Effects) Summary() Summary {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := Summary{
		Overlaps:   f.overlaps,
		MaxRunning: f.maxRunning,
		Count:      map[[2]string]int{},
	}
	for k, n := range f.count {
		s.Count[[2]string{k.agentID, k.eventID}] = n
	}
	return s
}
