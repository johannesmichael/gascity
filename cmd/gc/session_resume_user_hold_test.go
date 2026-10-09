package main

import (
	"context"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/clock"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

// J27 A: `gc session attach` of a session `gc session suspend` held and
// drained is an operator's own resume. It consumes the hold, so legacy's
// controller does not drain the attached session again. Real legacy ticks,
// with the production drain ops persisted across them.

// heldDrainedSession is a desired session the managed suspend held, whose
// drain the agent acked and whose runtime is gone: the row J27 A attaches to.
func heldDrainedSession(t *testing.T) (*reconcilerTestEnv, beads.Bead, drainOps) {
	t.Helper()
	env := newReconcilerTestEnv()
	env.cfg = &config.City{Agents: []config.Agent{{Name: "worker"}}}
	env.addDesired("worker", "worker", true)
	b := env.createSessionBead("worker", "worker")
	env.markSessionActive(&b)
	env.clk.Time = env.clk.Time.Add(30 * time.Minute)
	front := sessionFrontDoor(env.store)
	if err := front.ApplyPatch(b.ID, session.OperatorSuspendPatch(env.clk.Now())); err != nil {
		t.Fatal(err)
	}
	if err := env.sp.Stop("worker"); err != nil {
		t.Fatal(err)
	}
	if err := front.ApplyPatch(b.ID, session.AcknowledgeDrainPatch(env.clk.Now(), false)); err != nil {
		t.Fatal(err)
	}
	return env, b, newDrainOps(env.sp)
}

func (e *reconcilerTestEnv) userHoldTick(t *testing.T, id string, dops drainOps) {
	t.Helper()
	got, err := e.store.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	reconcileSessionBeads(
		context.Background(), []beads.Bead{got}, e.desiredState,
		configuredSessionNames(e.cfg, "", e.store), e.cfg, e.sp, e.store,
		dops, nil, nil, e.dt, map[string]int{"worker": 1}, false, nil, "",
		nil, e.clk, e.rec, 0, 0, &e.stdout, &e.stderr, e.startOptions...,
	)
}

// assertResumedStaysUp runs n ticks a drain timeout apart and fails if any
// stops the resumed runtime, or the hold or a drain survives them.
func (e *reconcilerTestEnv) assertResumedStaysUp(t *testing.T, id string, dops drainOps, n int) {
	t.Helper()
	stops := e.sp.CountCalls("Stop", "worker")
	for i := 0; i < n; i++ {
		e.clk.Time = e.clk.Time.Add(defaultDrainTimeout + time.Minute)
		e.userHoldTick(t, id, dops)
		if !e.sp.IsRunning("worker") || e.sp.CountCalls("Stop", "worker") != stops {
			t.Fatalf("tick %d stopped the resumed runtime; stdout:\n%s", i+1, e.stdout.String())
		}
	}
	info := e.sessionInfo(id)
	if info.SleepIntent != "" || info.HeldUntil != "" || e.dt.get(id) != nil {
		t.Fatalf("row sleep_intent=%q held_until=%q drain=%+v, want the hold consumed and no drain", info.SleepIntent, info.HeldUntil, e.dt.get(id))
	}
}

func TestAttachAfterManagedSuspendStaysUp(t *testing.T) {
	env, b, dops := heldDrainedSession(t)
	mgr := session.NewManagerWithOptions(env.store, env.sp, session.WithClock(env.clk))
	if err := mgr.Attach(context.Background(), b.ID, "test-cmd", runtime.Config{}); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	env.assertResumedStaysUp(t, b.ID, dops, 6)
}

// A user-hold drain the controller still tracks, past its deadline, is
// released once a resume consumed the hold: no forced stop, no completion
// written over the resumed row. A drain whose row still holds the intent
// runs as before.
func TestUserHoldDrainReleasedOnceTheHoldIsConsumed(t *testing.T) {
	for intent, wantStop := range map[string]bool{"": false, "user-hold": true} {
		now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
		sp := runtime.NewFake()
		if err := sp.Start(context.Background(), "s-held", runtime.Config{}); err != nil {
			t.Fatal(err)
		}
		store := beads.NewMemStore()
		b, err := store.Create(beads.Bead{Title: "worker", Type: sessionBeadType, Labels: []string{sessionBeadLabel}, Metadata: map[string]string{
			"session_name": "s-held", "template": "worker", "generation": "1", "state": "active", "sleep_intent": intent,
		}})
		if err != nil {
			t.Fatal(err)
		}
		dt := newDrainTracker()
		dt.set(b.ID, &drainState{startedAt: now.Add(-time.Hour), deadline: now.Add(-time.Minute), reason: "user-hold", generation: 1, ackSet: true})
		advanceSessionDrainsWithSessionsTraced(dt, sp, store, infoLookupFromBeadLookup(func(id string) *beads.Bead {
			got, _ := store.Get(id)
			return &got
		}), map[string]wakeEvaluation{}, &config.City{}, &clock.Fake{Time: now}, nil)
		if stopped := sp.CountCalls("Stop", "s-held") > 0; stopped != wantStop || dt.get(b.ID) != nil {
			t.Errorf("sleep_intent %q: stopped=%v tracked=%v, want stopped=%v and the drain done", intent, stopped, dt.get(b.ID) != nil, wantStop)
		}
	}
}

// tickOnStart runs one legacy tick right after the provider Start call,
// before the resume confirms it: the mid-resume race.
type tickOnStart struct {
	*runtime.Fake
	tick func()
}

func (p *tickOnStart) Start(ctx context.Context, name string, cfg runtime.Config) error {
	err := p.Fake.Start(ctx, name, cfg)
	if tick := p.tick; tick != nil {
		p.tick = nil
		tick()
	}
	return err
}

// A tick between the resume's Start and its consume sees a held row with a
// live runtime, so it begins a user-hold drain and publishes its ack. The
// consume still lands (the tick wrote nothing it reads), and the next ticks
// release that drain and its ack instead of honoring them.
func TestAttachAfterManagedSuspendSurvivesAMidResumeTick(t *testing.T) {
	env, b, dops := heldDrainedSession(t)
	sp := &tickOnStart{Fake: env.sp}
	sp.tick = func() {
		env.userHoldTick(t, b.ID, dops)
		if ds := env.dt.get(b.ID); ds == nil || ds.reason != "user-hold" {
			t.Errorf("mid-resume tick drain = %+v, want a user-hold drain (test premise)", ds)
		}
	}
	mgr := session.NewManagerWithOptions(env.store, sp, session.WithClock(env.clk))
	if err := mgr.Attach(context.Background(), b.ID, "test-cmd", runtime.Config{}); err != nil {
		t.Fatalf("Attach: %v", err)
	}
	if sp.tick != nil {
		t.Fatal("the mid-resume tick did not run (test premise)")
	}
	env.assertResumedStaysUp(t, b.ID, dops, 6)
}
