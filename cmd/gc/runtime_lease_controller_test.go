package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
	sessionpkg "github.com/gastownhall/gascity/internal/session"
)

// TestLockRuntimeNameTakesTheNameFlock: a v2 effect's name lock excludes a
// holder of the name's flock in another process.
func TestLockRuntimeNameTakesTheNameFlock(t *testing.T) {
	city := t.TempDir()
	flock, err := sessionpkg.TryRuntimeLease(nil, sessionpkg.RuntimeLeaseRequest{City: city, Name: "named-1"})
	if err != nil {
		t.Fatal(err)
	}
	w := &World{CityPath: city}
	if _, unlock, ok := lockRuntimeName(w, sessionpkg.Info{SessionName: "named-1"}); ok || unlock != nil {
		t.Fatal("lockRuntimeName ignored the name's flock")
	}
	flock.Release()
	_, unlock, ok := lockRuntimeName(w, sessionpkg.Info{SessionName: "named-1"})
	if !ok {
		t.Fatal("lockRuntimeName refused a free name")
	}
	if _, err := sessionpkg.TryRuntimeLease(nil, sessionpkg.RuntimeLeaseRequest{City: city, Name: "named-1"}); !errors.Is(err, sessionpkg.ErrRuntimeLeaseBusy) {
		t.Fatalf("flock under an effect's lock = %v, want busy", err)
	}
	unlock()
	unlock()
}

// TestControllerKillsNeverWait (M1): the controller's kill takes the runtime
// lease without waiting and reports a busy one as ErrRuntimeLeaseBusy for the
// caller to defer; an operator's kill waits, then fails retryably.
func TestControllerKillsNeverWait(t *testing.T) {
	store, bead, city := newKillPokeSession(t, "s-gc-ctl-kill")
	defer sessionpkg.SetOperatorLeaseWaitForTest(400 * time.Millisecond)()
	inner := wrapKillPokeProvider(t, &killHookProvider{})
	held, err := sessionpkg.TryRuntimeLease(sessionFrontDoor(store), sessionpkg.RuntimeLeaseRequest{
		City: city, Name: "s-gc-ctl-kill", ID: bead.ID, TTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	sp, err := newSessionProvider()
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	err = controllerKillSessionTarget(city, store, sp, nil, bead.ID)
	if !errors.Is(err, sessionpkg.ErrRuntimeLeaseBusy) || errors.Is(err, sessionpkg.ErrSessionStarting) || time.Since(began) > 300*time.Millisecond {
		t.Fatalf("controller kill = %v after %v, want ErrRuntimeLeaseBusy at once", err, time.Since(began))
	}
	began = time.Now()
	if err := workerKillSessionTargetWithConfig(city, store, sp, nil, bead.ID); !errors.Is(err, sessionpkg.ErrSessionStarting) || time.Since(began) < 150*time.Millisecond {
		t.Fatalf("operator kill = %v after %v, want ErrSessionStarting after the wait", err, time.Since(began))
	}
	if !inner.IsRunning("s-gc-ctl-kill") {
		t.Fatal("a kill stopped the runtime under another holder's lease")
	}
}

// TestKillsRunUnderTheCitysTTL pins the worker factory's lease TTL: the
// city's startup timeout plus the margin.
func TestKillsRunUnderTheCitysTTL(t *testing.T) {
	store, bead, city := newKillPokeSession(t, "s-gc-kill-ttl")
	var ttl string
	wrapKillPokeProvider(t, &killHookProvider{beforeStop: func() { ttl = mustGetBead(t, store, bead.ID).Metadata[sessionpkg.RuntimeLeaseTTLKey] }})
	sp, err := newSessionProvider()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{Session: config.SessionConfig{StartupTimeout: "3m"}}
	if err := controllerKillSessionTarget(city, store, sp, cfg, bead.ID); err != nil {
		t.Fatal(err)
	}
	if ttl != "240" {
		t.Fatalf("lease TTL at the stop = %q, want 240 (3m + the margin)", ttl)
	}
}

// TestControllerStopSequencesDeferOnABusyLease (M1): the async drain-ack stop
// and the escalation take the lease once for all their kills; a busy lease
// defers them before any stop, and the escalation never reaches its
// process-table kill.
func TestControllerStopSequencesDeferOnABusyLease(t *testing.T) {
	for _, seq := range []string{"async stop", "escalation"} {
		t.Run(seq, func(t *testing.T) {
			city := t.TempDir()
			sp := runtime.NewFake()
			if err := sp.Start(context.Background(), "worker", runtime.Config{Command: "x"}); err != nil {
				t.Fatal(err)
			}
			flock, err := sessionpkg.TryRuntimeLease(nil, sessionpkg.RuntimeLeaseRequest{City: city, Name: "worker"})
			if err != nil {
				t.Fatal(err)
			}
			defer flock.Release()
			var stderr synchronizedBuffer
			tracker := &asyncStartTracker{}
			if seq == "async stop" {
				queueDrainAckAsyncStop(city, beads.NewMemStore(), sp, &config.City{}, "sess-1", "worker", "", nil, tracker, nil, &stderr)
			} else {
				done, _ := tracker.startDrainAckStop("escalate:sess-1")
				queueDrainAckForcedTermination(city, beads.NewMemStore(), sp, &config.City{}, sessionpkg.Info{ID: "sess-1"}, "worker",
					"agent_acked_runtime_survived", 1, time.Now(), nil, 0, done, nil, &stderr)
			}
			if !tracker.wait(5 * time.Second) {
				t.Fatal("the stop sequence never finished")
			}
			if !strings.Contains(stderr.String(), "deferred") || sp.CountCalls("Stop", "worker") != 0 || !sp.IsRunning("worker") {
				t.Fatalf("stderr %q, stops %d; want deferred with no stop", stderr.String(), sp.CountCalls("Stop", "worker"))
			}
			if strings.Contains(stderr.String(), "terminat") {
				t.Fatalf("stderr %q: the escalation reached its process-table kill", stderr.String())
			}
		})
	}
}

// TestConfigDriftResetDefersOnABusyLease: a config-drift reset whose stop is
// refused by another holder's lease decides nothing this tick: no patch, the
// runtime and the row as they were.
func TestConfigDriftResetDefersOnABusyLease(t *testing.T) {
	env := newReconcilerTestEnv()
	session := env.createSessionBead("mayor", "mayor")
	if err := env.sp.Start(context.Background(), "mayor", runtime.Config{Command: "x"}); err != nil {
		t.Fatal(err)
	}
	city := t.TempDir()
	held, err := sessionpkg.TryRuntimeLease(sessionFrontDoor(env.store), sessionpkg.RuntimeLeaseRequest{City: city, Name: "mayor", ID: session.ID, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	before := mustGetBead(t, env.store, session.ID).Metadata["state"]
	patch := resetConfiguredNamedSessionForConfigDriftInfo(city, env.sessionInfo(session.ID), driftResumeSessionIDCapableTemplateParams(),
		env.store, env.sp, "mayor", true, "creating", time.Now().UTC(), &env.stderr)
	if patch != nil || !env.sp.IsRunning("mayor") || mustGetBead(t, env.store, session.ID).Metadata["state"] != before {
		t.Fatalf("drift reset under a held lease = %v (running %v); want nothing decided", patch, env.sp.IsRunning("mayor"))
	}
}

// TestVerifiedStopNeverWaits: the drain's verified stop is the controller's,
// and defers on a busy lease at once.
func TestVerifiedStopNeverWaits(t *testing.T) {
	store, bead, city := newKillPokeSession(t, "s-gc-verified")
	defer sessionpkg.SetOperatorLeaseWaitForTest(2 * time.Second)()
	wrapKillPokeProvider(t, &killHookProvider{})
	held, err := sessionpkg.TryRuntimeLease(sessionFrontDoor(store), sessionpkg.RuntimeLeaseRequest{City: city, Name: "s-gc-verified", ID: bead.ID, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	sp, err := newSessionProvider()
	if err != nil {
		t.Fatal(err)
	}
	info, err := sessionFrontDoor(store).Get(bead.ID)
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	if err := verifiedStop(city, info, store, sp, nil); !errors.Is(err, sessionpkg.ErrRuntimeLeaseBusy) || time.Since(began) > time.Second {
		t.Fatalf("verifiedStop under a held lease = %v after %v, want busy at once", err, time.Since(began))
	}
}
