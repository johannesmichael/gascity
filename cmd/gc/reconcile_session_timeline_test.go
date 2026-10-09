package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/events"
)

// The timeline differential (ARCH-RESTRUCTURE R6.1). The session
// differential compares legacy and v2 at one frozen instant, which hid every
// rule that reads the clock (the idle latch, detached_at, the stability
// clears). A timeline fixture is a parity fixture plus steps: each advances
// both clocks, applies one outside event to both copies, then runs the
// copies in lockstep, one legacy tick and one v2 pass a patrol apart, until
// neither changes anything. Legacy's drain tracker and async stops, the
// simulator's caches and both providers carry across steps, as a
// controller's do across ticks.
//
// After a step the copies compare on every aspect of their outcome: each
// row's and work bead's status, type, labels and metadata (both legs), each
// runtime, and the step's provider calls and events with their payload
// values. Times are normalized to offsets from parityNow, paths and config
// hashes elided. A difference is accounted as in the session differential,
// but per step: every Explain key reads "@<step> <aspect>", and one no step
// shows fails. The values of the explained differences are pinned too
// (testdata/session_differential/timeline_explained.golden), so an explained
// aspect whose v2 value moves fails as well.
//
// Every fixture seeds a census-only row on the rig leg (C5D2-1), keeps
// titles, session names and aliases distinct (E3B-S1), and seeds
// last_woke_at (C5D2-2).
//
// The legacy golden (owner ruling O7): what legacy changed at each step of
// each fixture, the step's own outside write excluded, is checked in as
// testdata/session_differential/timeline.golden. The test fails when legacy
// differs from it; `go test -run TestSessionTimelineDifferential
// -update-golden` rewrites both goldens, and the test then fails until
// timelineGoldenRulings admits the fixture's new section, by digest, under
// a ruling (timelineRulingRefs or a "§12.2 row N").

var updateTimelineGolden = flag.Bool("update-golden", false, "rewrite the timeline differential's goldens from the copies' outcomes")

const (
	timelineGoldenPath    = "testdata/session_differential/timeline.golden"
	timelineExplainedPath = "testdata/session_differential/timeline_explained.golden"
	// timelineTicks bounds a step's lockstep run to a fixed point.
	timelineTicks = 16
)

// timelineFixture is a parity fixture run as a sequence of steps after the
// seed step. Setup, when set, prepares each copy before the seed.
type timelineFixture struct {
	parityFixture
	Setup func(tw twin)
	Steps []parityStep
}

// steps is f's steps after the seed step.
func (f timelineFixture) steps() []parityStep {
	if len(f.Steps) > 0 && f.Steps[0].Name == "seed" {
		return f.Steps
	}
	return append([]parityStep{{Name: "seed"}}, f.Steps...)
}

// parityStep advances both clocks by Advance, or to At after parityNow,
// then applies Op to each copy and runs them to a fixed point, or for Ticks
// lockstep ticks when set. A fixture's first step named "seed" replaces the
// default seed step.
type parityStep struct {
	Name    string
	Advance time.Duration
	At      time.Duration
	Ticks   int
	Op      func(tw twin)
}

// twin is one copy as an outside writer sees it: its config and reload,
// provider, stores (the city leg, then the rig leg) and recorder.
type twin struct {
	t      *testing.T
	cfg    *config.City
	reload func(*config.City)
	sp     *simProvider
	stores []beads.Store
	rec    events.Recorder
	now    time.Time
}

// row finds id on either leg.
func (tw twin) row(id string) (beads.Store, beads.Bead) {
	for _, st := range tw.stores {
		if b, err := st.Get(id); err == nil {
			return st, b
		}
	}
	tw.t.Fatalf("timeline: no row %s", id)
	return nil, beads.Bead{}
}

// runtime changes the runtime under row id's session name; a nil result
// removes it.
func (tw twin) runtime(id string, fn func(rt simRuntime) *simRuntime) {
	_, b := tw.row(id)
	name := b.Metadata["session_name"]
	tw.sp.mu.Lock()
	defer tw.sp.mu.Unlock()
	rt := tw.sp.rts[name]
	if rt == nil {
		tw.t.Fatalf("timeline: no runtime under %s", name)
	}
	if next := fn(*rt); next == nil {
		tw.sp.drop(name)
	} else {
		*rt = *next
		tw.sp.touch(name)
	}
}

// twin is copy A's view; a reload swaps the config its next tick reads.
func (w *legacyWorld) twin(t *testing.T) twin {
	stores := []beads.Store{w.store}
	for _, rig := range slices.Sorted(maps.Keys(w.rigs)) {
		stores = append(stores, w.rigs[rig])
	}
	return twin{t: t, cfg: w.cfg, reload: func(c *config.City) { w.cfg = c }, sp: w.sp.simProvider, stores: stores, rec: w.rec, now: w.clk.Now()}
}

// twin is copy B's view; it writes the backings, and a reload publishes a
// new environment generation.
func (w *v2World) twin(t *testing.T) twin {
	var stores []beads.Store
	for _, l := range w.s.legs {
		stores = append(stores, l.backing)
	}
	reload := func(c *config.City) {
		env := w.s.env.Env()
		next := *env
		next.Gen, next.Cfg = env.Gen+1, c
		*env, w.s.cfg = next, c
	}
	return twin{t: t, cfg: w.s.cfg, reload: reload, sp: w.s.sp, stores: stores, rec: w.rec, now: w.s.clk.Now()}
}

// delivered queues the outside writes' events and delivers each to its
// leg's cache, as a quiet controller's event stream would.
func (w *v2World) delivered() {
	w.s.audit("timeline")
	for _, l := range w.s.legs {
		for _, ev := range l.events {
			l.cache.ApplyEvent("bead.updated", ev)
		}
		l.events = nil
		l.cache.ReconcileNowForTest()
	}
}

// settleTimeline runs a legacy tick and a v2 pass, then advances both
// clocks a patrol, until a tick and a pass both change nothing, or for
// ticks lockstep ticks when set.
func settleTimeline(t *testing.T, name string, lw *legacyWorld, vw *v2World, ticks int) {
	t.Helper()
	for i := 0; ; i++ {
		lchanged, vchanged := lw.once(t), vw.once()
		if ticks > 0 && i+1 == ticks || ticks == 0 && !lchanged && !vchanged {
			return
		}
		if i == timelineTicks {
			t.Fatalf("%s: no lockstep fixed point in %d patrols (legacy changed %t, v2 %t)", name, timelineTicks, lchanged, vchanged)
		}
		lw.clk.Time = lw.clk.Time.Add(simPatrol)
		vw.s.advance(simPatrol)
	}
}

// runtimesOf is each runtime by name, its identity and shape.
func runtimesOf(p *simProvider) map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]string{}
	for name, rt := range p.rts {
		shape := "alive"
		switch {
		case rt.corpse:
			shape = "corpse"
		case rt.zombie:
			shape = "zombie"
		}
		token := rt.token
		if token != "" && !strings.HasPrefix(token, "tok-") {
			token = "<set>"
		}
		out[name] = fmt.Sprintf("id=%s epoch=%s token=%s %s attached=%t", rt.id, rt.epoch, token, shape, rt.attached)
	}
	return out
}

var timelineTimes = regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[+-]\d\d:\d\d)`)

// normalizer renders a value for comparison and the goldens: paths elided,
// times as offsets from parityNow (a wall-clock time as <wall>), and config
// hashes, which digest the paths, as <set>.
type normalizer struct{ paths *strings.Replacer }

func (n normalizer) value(key, v string) string {
	if v != "" && (strings.HasSuffix(key, "_hash") || strings.HasSuffix(key, "_hash_breakdown")) {
		return "<set>"
	}
	return timelineTimes.ReplaceAllStringFunc(n.paths.Replace(v), func(s string) string {
		at, err := time.Parse(time.RFC3339Nano, s)
		d := at.Sub(parityNow)
		switch {
		case err != nil || d > 24*time.Hour || d < -24*time.Hour:
			return "<wall>"
		case d < 0:
			return "T-" + (-d).String()
		}
		return "T+" + d.String()
	})
}

// view flattens o, its calls and events counted from prev on, into
// aspect → value: each row's and work bead's presence, status, type, labels,
// assignee and metadata keys, each runtime, and each call and event.
func (n normalizer) view(o, prev parityOutcome, runtimes map[string]string) map[string]string {
	out := map[string]string{}
	bead := func(kind string, b beads.Bead) {
		p := kind + ":" + b.ID
		out[p], out[p+":status"], out[p+":type"] = "present", b.Status, b.Type
		if len(b.Labels) > 0 {
			out[p+":labels"] = strings.Join(slices.Sorted(slices.Values(b.Labels)), ",")
		}
		if b.Assignee != "" {
			out[p+":assignee"] = n.value("", b.Assignee)
		}
		for k, v := range b.Metadata {
			if v != "" {
				out[p+":"+k] = n.value(k, v)
			}
		}
	}
	for _, b := range o.rows {
		bead("row", b)
	}
	for _, b := range o.work {
		bead("work", b)
	}
	for name, rt := range runtimes {
		out["runtime:"+name] = rt
	}
	for a, c := range o.actions {
		if d := c - prev.actions[a]; d != 0 {
			out["provider:"+a] = fmt.Sprint(d)
		}
	}
	for e, c := range o.payloads {
		if d := c - prev.payloads[e]; d != 0 {
			out["event:"+n.value("", e)] = fmt.Sprint(d)
		}
	}
	return out
}

// counted reports whether aspect is a step's call or event, not state.
func counted(aspect string) bool {
	return strings.HasPrefix(aspect, "provider:") || strings.HasPrefix(aspect, "event:")
}

// goldenLines renders what changed from prev to cur: each state aspect that
// differs ("aspect=" clears it), then each call and event.
func goldenLines(cur, prev map[string]string) string {
	var b strings.Builder
	for _, a := range unionKeys(cur, prev) {
		if counted(a) {
			if cur[a] != "" {
				fmt.Fprintf(&b, "%s=%s\n", a, cur[a])
			}
		} else if cur[a] != prev[a] {
			fmt.Fprintf(&b, "%s=%s\n", a, cur[a])
		}
	}
	return b.String()
}

// runTimeline runs f's steps on both copies, accounts every difference
// after every step, and returns legacy's golden section and the explained
// differences' section.
func runTimeline(t *testing.T, f timelineFixture) (legacy, explained string) {
	t.Helper()
	for key := range f.Explain {
		if !strings.HasPrefix(key, "@") {
			t.Errorf("%s: Explain key %q names no step (\"@<step> <aspect>\")", f.Name, key)
		}
	}
	cityPath := t.TempDir()
	rigPath := filepath.Join(cityPath, "rigs", simRigLeg)
	if err := os.MkdirAll(rigPath, 0o755); err != nil {
		t.Fatal(err)
	}
	rows := f.converged(t, cityPath)
	lw := newLegacyWorld(f.parityFixture, cityPath, cloneDiffBeads(rows))
	lw.cfg.Rigs = []config.Rig{{Name: simRigLeg, Path: rigPath}}
	lw.rigs = map[string]beads.Store{simRigLeg: beads.NewMemStoreFrom(0, cloneDiffBeads(f.Rig), nil)}
	vw := newV2World(t, f.parityFixture, cityPath, rows, append([]beads.Bead{}, f.Rig...), rigPath)
	if f.Setup != nil {
		f.Setup(lw.twin(t))
		f.Setup(vw.twin(t))
		vw.delivered()
	}
	n := normalizer{strings.NewReplacer(rigPath, "<rig>", cityPath, "<city>")}

	// The seed's golden shows legacy's changes to the fixture's rows, its
	// session-bead sync's convergence included; a later step's, its changes
	// since the step's own outside write.
	raw := newParityOutcome(cloneDiffBeads(append(f.beads(), f.Rig...)), lw.sp, &memRecorder{})
	prevState := n.view(raw, raw, runtimesOf(lw.sp.simProvider))
	var lsec, xsec strings.Builder
	shown := map[string]bool{}
	prevExplained := map[string]string{} // aspect → its explained values, the previous step's
	for i, st := range f.steps() {
		advance := st.Advance
		if st.At > 0 {
			if advance = parityNow.Add(st.At).Sub(lw.clk.Now()); advance < 0 {
				t.Fatalf("%s, step %s: at +%s, but the clock is at +%s", f.Name, st.Name, st.At, lw.clk.Now().Sub(parityNow))
			}
		}
		if advance > 0 {
			lw.clk.Time = lw.clk.Time.Add(advance)
			vw.s.advance(advance)
		}
		if st.Op != nil {
			st.Op(lw.twin(t))
			st.Op(vw.twin(t))
			vw.delivered()
		}
		lpost, vpost := lw.outcome(t), vw.outcome(t)
		if st.Op != nil {
			prevState = n.view(lpost, lpost, runtimesOf(lw.sp.simProvider))
		}
		settleTimeline(t, f.Name, lw, vw, st.Ticks)
		lout, vout := lw.outcome(t), vw.outcome(t)
		l, v := n.view(lout, lpost, runtimesOf(lw.sp.simProvider)), n.view(vout, vpost, runtimesOf(vw.s.sp))
		header := fmt.Sprintf("## %d %s (+%s)\n", i, st.Name, lw.clk.Now().Sub(parityNow))
		lsec.WriteString(header + goldenLines(l, prevState))
		xsec.WriteString(header)
		prevState = l
		explained := map[string]string{}
		for _, aspect := range unionKeys(l, v) {
			if l[aspect] == v[aspect] {
				continue
			}
			key, ok := f.explains("@" + st.Name + " " + aspect)
			if !ok {
				t.Errorf("%s, step %d %s: unexplained difference %s (legacy %q, v2 %q)", f.Name, i, st.Name, aspect, l[aspect], v[aspect])
				continue
			}
			shown[key] = true
			explained[aspect] = fmt.Sprintf("legacy=%q v2=%q [%s]", l[aspect], v[aspect], f.Explain[key])
			switch table, e, ok := parityEntryOf(f.Explain[key]); {
			case !ok:
				t.Errorf("%s: %s accounted to %q, which no table lists", f.Name, aspect, f.Explain[key])
			case table == "finding":
				t.Logf("%s, step %d %s: %s is open finding %s (%s)", f.Name, i, st.Name, aspect, f.Explain[key], e.ref)
			}
		}
		// The explained differences are pinned as they change: each new or
		// moved one with its values, and each one that no longer shows.
		for _, aspect := range unionKeys(explained, prevExplained) {
			if explained[aspect] == "" {
				fmt.Fprintf(&xsec, "%s resolved\n", aspect)
			} else if explained[aspect] != prevExplained[aspect] {
				fmt.Fprintf(&xsec, "%s %s\n", aspect, explained[aspect])
			}
		}
		prevExplained = explained
	}
	for key, id := range f.Explain {
		if !shown[key] {
			t.Errorf("%s: %s, accounted to %s, does not show: legacy and v2 agree", f.Name, key, id)
		}
	}
	return lsec.String(), xsec.String()
}

// goldenRuling admits one golden section: the fixture, the ruling that
// admits it, and the section's digest.
type goldenRuling struct{ fixture, ruling, digest string }

// timelineGoldenBaseline admits legacy's outcomes as this PR found them; it
// may only be a fixture's first entry.
const timelineGoldenBaseline = "O7 baseline: legacy at bfe4f38eaa (TL)"

// timelineRulingRefs are the rulings a later entry may name: the owner's
// O-rulings (O7 itself admits no change), and CONTRACT §12.2 rows in the
// form "§12.2 row N" (timelineSection122).
var (
	timelineRulingRefs = []string{"O1", "O2", "O3", "O4", "O5", "O6", "O8", "O9", "O10"}
	timelineSection122 = regexp.MustCompile(`§12\.2 row \d+\b`)
)

// timelineGoldenRulings admit each fixture's golden section. A change to
// legacy's outcome on a fixture appends an entry naming its ruling and the
// new digest; the last entry per fixture is the one in force.
var timelineGoldenRulings = []goldenRuling{
	{"a crash 29s after wake", timelineGoldenBaseline, "de882b5af5da"},
	{"a crash 31s after wake", timelineGoldenBaseline, "ca87e99b0987"},
	{"a crash 5m0s after wake", timelineGoldenBaseline, "9fb7374990bf"},
	{"a live quarantined row 29s after wake", timelineGoldenBaseline, "0651d1456a1b"},
	{"a live quarantined row 31s after wake", timelineGoldenBaseline, "7118b3924da0"},
	{"an unwanted row inside, then past, the wake grace", timelineGoldenBaseline, "bead46681605"},
	{"attach, then detach, then idle", timelineGoldenBaseline, "e2595b43284b"},
	{"idle, then sleep, then the next pass", timelineGoldenBaseline, "2e9ca891738c"},
}

func goldenDigest(section string) string {
	sum := sha256.Sum256([]byte(section))
	return hex.EncodeToString(sum[:6])
}

// namesRuling reports whether a later ruling names one of the fixed refs.
func namesRuling(ruling string) bool {
	for _, w := range strings.FieldsFunc(ruling, func(r rune) bool { return r == ' ' || r == ',' || r == ';' || r == '(' || r == ')' }) {
		if slices.Contains(timelineRulingRefs, w) {
			return true
		}
	}
	return timelineSection122.MatchString(ruling)
}

// unruledGolden lists the sections no ruling admits and the rulings out of
// form: one for no fixture, a baseline that is not its fixture's first
// entry, or a later entry that names no ruling.
func unruledGolden(sections map[string]string, rulings []goldenRuling) []string {
	var out []string
	inForce := map[string]goldenRuling{}
	for _, r := range rulings {
		_, first := inForce[r.fixture]
		first = !first
		switch {
		case sections[r.fixture] == "":
			out = append(out, fmt.Sprintf("ruling %q names no fixture %q", r.ruling, r.fixture))
		case first && r.ruling != timelineGoldenBaseline:
			out = append(out, fmt.Sprintf("%q's first ruling %q is not the baseline", r.fixture, r.ruling))
		case !first && (r.ruling == timelineGoldenBaseline || !namesRuling(r.ruling)):
			out = append(out, fmt.Sprintf("ruling %q for %q names none of %v nor a \"§12.2 row N\"", r.ruling, r.fixture, timelineRulingRefs))
		}
		inForce[r.fixture] = r
	}
	for _, name := range slices.Sorted(maps.Keys(sections)) {
		if d := goldenDigest(sections[name]); inForce[name].digest != d {
			out = append(out, fmt.Sprintf("golden section %q (digest %s) is admitted by no ruling: append {%q, \"<ruling>\", %q}", name, d, name, d))
		}
	}
	return out
}

// parseGolden splits a golden file into sections by fixture.
func parseGolden(text string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(text, "\n=== ")[1:] {
		name, body, _ := strings.Cut(part, "\n")
		out[name] = strings.TrimSuffix(body, "\n") + "\n"
	}
	return out
}

func renderGolden(header string, sections map[string]string) string {
	var b strings.Builder
	b.WriteString(header)
	for _, name := range slices.Sorted(maps.Keys(sections)) {
		fmt.Fprintf(&b, "\n=== %s\n%s", name, strings.TrimSuffix(sections[name], "\n"))
	}
	b.WriteString("\n")
	return b.String()
}

// checkGolden compares the sections a run produced with path's, rewriting
// them under -update-golden; sections a -run filter skipped are kept. It
// returns the file's sections.
func checkGolden(t *testing.T, path, header string, got map[string]string) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil && !*updateTimelineGolden {
		t.Fatal(err)
	}
	want := parseGolden(string(raw))
	if *updateTimelineGolden {
		next := map[string]string{}
		for _, f := range timelineFixtures() {
			if section, ok := got[f.Name]; ok {
				next[f.Name] = section
			} else if section, ok := want[f.Name]; ok {
				next[f.Name] = section
			}
		}
		if err := os.WriteFile(path, []byte(renderGolden(header, next)), 0o644); err != nil {
			t.Fatal(err)
		}
		want = next
	}
	for _, name := range slices.Sorted(maps.Keys(got)) {
		if got[name] != want[name] {
			t.Errorf("%s: %q differs: if a ruling explains it, rerun with -update-golden (and for legacy, add the ruling to timelineGoldenRulings)\n--- golden\n%s\n--- got\n%s", path, name, want[name], got[name])
		}
	}
	return want
}

// Kills a clock-dependent divergence between legacy and v2 that a frozen
// instant cannot show, a moved v2 value under an explained difference, and
// a silent change to legacy's timeline behavior: every difference after
// every step is accounted and its values pinned, legacy's changes equal the
// golden, and a ruling admits every golden section.
func TestSessionTimelineDifferential(t *testing.T) {
	legacy, explained := map[string]string{}, map[string]string{}
	for _, f := range timelineFixtures() {
		t.Run(f.Name, func(t *testing.T) { legacy[f.Name], explained[f.Name] = runTimeline(t, f) })
	}
	want := checkGolden(t, timelineGoldenPath, "# What legacy changed at each step of each timeline fixture (reconcile_session_timeline_test.go).\n"+
		"# Owner ruling O7: a change here needs an entry in timelineGoldenRulings. Regenerate with -update-golden.\n", legacy)
	checkGolden(t, timelineExplainedPath, "# The explained legacy/v2 differences after each step of each timeline fixture, with their values.\n"+
		"# Regenerate with -update-golden when an arm's port moves them.\n", explained)
	for _, msg := range unruledGolden(want, timelineGoldenRulings) {
		t.Error(msg)
	}
}

// Kills a golden that changes with no ruling, or under a ruling out of form:
// the baseline only as a fixture's first entry, a later entry naming a fixed
// ruling or a §12.2 row, and the last entry carrying the section's digest.
func TestTimelineGoldenNeedsARuling(t *testing.T) {
	sections := map[string]string{"a": "## 0 seed\nrow:gc-1=present\n", "b": "## 0 seed\n"}
	da, db := goldenDigest(sections["a"]), goldenDigest(sections["b"])
	base := timelineGoldenBaseline
	for _, tc := range []struct {
		name    string
		rulings []goldenRuling
		want    int
	}{
		{"every section ruled", []goldenRuling{{"a", base, da}, {"b", base, db}}, 0},
		{"a section unruled", []goldenRuling{{"a", base, da}}, 1},
		{"a changed section under its old ruling", []goldenRuling{{"a", base, "000000000000"}, {"b", base, db}}, 1},
		{"a later O-ruling supersedes", []goldenRuling{{"a", base, "000000000000"}, {"a", "O4 (polarity)", da}, {"b", base, db}}, 0},
		{"a later §12.2 row supersedes", []goldenRuling{{"a", base, "000000000000"}, {"a", "CONTRACT §12.2 row 25", da}, {"b", base, db}}, 0},
		{"a later entry naming O7", []goldenRuling{{"a", base, "000000000000"}, {"a", "O7", da}, {"b", base, db}}, 1},
		{"a later baseline", []goldenRuling{{"a", base, "000000000000"}, {"a", base, da}, {"b", base, db}}, 1},
		{"a later entry naming nothing", []goldenRuling{{"a", base, "000000000000"}, {"a", "looks fine", da}, {"b", base, db}}, 1},
		{"a first entry that is not the baseline", []goldenRuling{{"a", "O4", da}, {"b", base, db}}, 1},
		{"a ruling for no fixture", []goldenRuling{{"a", base, da}, {"b", base, db}, {"c", base, db}}, 1},
	} {
		if got := unruledGolden(sections, tc.rulings); len(got) != tc.want {
			t.Errorf("%s: %d complaints %v, want %d", tc.name, len(got), got, tc.want)
		}
	}
	if got := parseGolden(renderGolden("# h\n", sections)); !maps.Equal(got, sections) {
		t.Errorf("golden round trip: %v, want %v", got, sections)
	}
}

// timelineFixtures are the clock-dependent families of ARCH-RESTRUCTURE
// §3.6, each group's own: the stability clears and accrual, then the idle
// and drain timers, then the operator verbs.
func timelineFixtures() []timelineFixture {
	return slices.Concat(stabilityTimeline(), idleTimeline())
}

func tlAt(d time.Duration) string { return parityNow.Add(d).UTC().Format(time.RFC3339) }

// tlRow is an awake pool row woken woke ago, whose title, session name and alias
// differ; the transient pool manages no alias, so legacy clears it.
func tlRow(id string, slot int, woke time.Duration, meta ...string) parityRow {
	b := poolRow(id, "worker", slot, "awake", append([]string{
		"instance_token", "tok-" + id, "last_woke_at", tlAt(-woke), "alias", "alias-" + id,
	}, meta...)...)
	b.Title = "Display " + id
	return b
}

func tlLive(id string) simRuntime { return simRuntime{id: id, epoch: "1", token: "tok-" + id} }

func tlClaim(status, assignee string) []parityRow {
	return []parityRow{{ID: "gw-1", Title: "gw-1", Type: "task", Status: status, Assignee: assignee, Metadata: map[string]string{"gc.routed_to": "worker"}}}
}

// tlCity names its tmux sessions apart from its agents.
func tlCity() *config.City {
	cfg := workerCity(3)
	cfg.Workspace.SessionTemplate = "{{.City}}--{{.Agent}}"
	return cfg
}

// tlFixture builds a timeline fixture over gc-1's city leg and the
// census-only rig-leg row rg-1 every fixture seeds. Its Explain is
// standing, scoped to every step, plus per, scoped to the step it is keyed
// by.
func tlFixture(name string, cfg func() *config.City, rows, work []parityRow, rts []simRuntime, steps []parityStep, standing map[string]string, per map[string]map[string]string) timelineFixture {
	explain := map[string]string{}
	for _, st := range (timelineFixture{Steps: steps}).steps() {
		for k, v := range standing {
			explain["@"+st.Name+" "+k] = v
		}
		for k, v := range per[st.Name] {
			explain["@"+st.Name+" "+k] = v
		}
	}
	rig := []parityRow{tlRow("rg-1", 11, time.Hour)}
	return timelineFixture{parityFixture: parityFixture{Name: name, City: cfg, Rows: rows, Work: work, Rig: rig, Runtimes: append(rts, tlLive("rg-1")), Explain: explain}, Steps: steps}
}

func tlMerged(ms ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range ms {
		maps.Copy(out, m)
	}
	return out
}

// The standing differences: legacy's sync converges row id's sleep-policy
// keys and identity, and stamps the run identity on in-progress work.
func tlPol(id string) map[string]string {
	return map[string]string{"row:" + id + ":*policy": "sleep-policy-keys"}
}

func tlIdent() map[string]string { return map[string]string{"row:gc-1:*identity": "A7 identity"} }

func tlStamp() map[string]string {
	return map[string]string{"work:gw-1:*stamp": "external-reads-stamp"}
}

// tlStartState is the row and runtime legacy's start of row id under name
// leaves, and tlStarted that start in its own step; v2 leaves both to A18.
func tlStartState(id, name string) map[string]string {
	return map[string]string{"row:" + id + ":*start": "A18 start", "runtime:" + name: "A18 start"}
}

func tlStarted(id, name, subject string) map[string]string {
	return tlMerged(tlStartState(id, name), map[string]string{"provider:Start " + name: "A18 start", "provider:RemoveMeta " + name: "A18 start", "event:session.woke " + subject + " null": "A18 start"})
}

func tlAccrued() map[string]string {
	return map[string]string{"row:gc-1:*accrual": "stability-accrual"}
}

// stabilityTimeline is SESS-537..540 through time, on a pool of one whose
// canonical alias "worker" legacy keeps and whose work is assigned by that
// alias.
func stabilityTimeline() []timelineFixture {
	stable := func() *config.City {
		cfg := tlCity()
		cfg.Agents[0].MaxActiveSessions, cfg.Agents[0].MinActiveSessions = intPtr(1), intPtr(0)
		return cfg
	}
	row := func(meta ...string) parityRow {
		b := tlRow("gc-1", 1, 0, meta...)
		b.Metadata["alias"], b.Metadata["agent_name"] = "worker", "worker"
		return b
	}
	standing := tlMerged(tlPol("gc-1"), tlIdent(), tlStamp())
	// crashAfter is a crash d after the wake of a row with prior wake
	// failures and churn: inside the 30s stability threshold legacy counts a
	// wake failure and restarts; past it a death before the productivity
	// threshold is churn.
	crashAfter := func(d time.Duration, crashed map[string]string) timelineFixture {
		crash := func(tw twin) { tw.runtime("gc-1", func(simRuntime) *simRuntime { return nil }) }
		return tlFixture(fmt.Sprintf("a crash %s after wake", d), stable,
			[]parityRow{row("wake_attempts", "2", "churn_count", "2")}, tlClaim("in_progress", "worker"), []simRuntime{tlLive("gc-1")},
			[]parityStep{{Name: "crash", At: d, Op: crash}}, standing, map[string]map[string]string{"crash": crashed})
	}
	// quarantinedAt is a live, quarantined row seen one tick at d after its
	// wake: past the 30s stability threshold legacy clears the wake failures
	// and the quarantine (SESS-539). Meanwhile its quarantine keeps it from
	// waking for its work, so legacy drains it, and its work vetoes the
	// drain.
	quarantinedAt := func(d time.Duration, per map[string]map[string]string) timelineFixture {
		return tlFixture(fmt.Sprintf("a live quarantined row %s after wake", d), stable,
			[]parityRow{row("wake_attempts", "2", "quarantined_until", tlAt(time.Hour))}, tlClaim("in_progress", "worker"), []simRuntime{tlLive("gc-1")},
			[]parityStep{{Name: "seed", Ticks: 1}, {Name: "alive", At: d, Ticks: 1}, {Name: "settled"}}, standing, per)
	}
	drainBegun := map[string]string{"row:gc-1:*drain": "A20 drain-begin", "provider:SetMeta s-gc-1": "A20 drain-begin"}
	restarted := tlStarted("gc-1", "s-gc-1", "worker")
	return []timelineFixture{
		crashAfter(29*time.Second, tlMerged(restarted, tlAccrued())),
		crashAfter(31*time.Second, tlMerged(tlAccrued(), map[string]string{"row:gc-1:last_woke_at": "stability-accrual"})),
		crashAfter(5*time.Minute, tlMerged(restarted, tlAccrued())),
		quarantinedAt(29*time.Second, map[string]map[string]string{
			"seed": drainBegun, "alive": {"provider:RemoveMeta s-gc-1": "A20 drain-begin"},
			"settled": {"provider:SetMeta s-gc-1": "A20 drain-begin", "provider:RemoveMeta s-gc-1": "A20 drain-begin"},
		}),
		quarantinedAt(31*time.Second, map[string]map[string]string{
			"seed": drainBegun, "alive": {"provider:RemoveMeta s-gc-1": "A20 drain-begin", "row:gc-1:*clear": "stability-clear-order"},
		}),
	}
}

// reconfigure reloads the copy with a changed copy of its config.
func (tw twin) reconfigure(change func(*config.City)) {
	next := *tw.cfg
	next.Agents = make([]config.Agent, len(tw.cfg.Agents))
	for i, a := range tw.cfg.Agents {
		next.Agents[i] = a.Clone()
	}
	next.NamedSessions, next.Rigs = slices.Clone(tw.cfg.NamedSessions), slices.Clone(tw.cfg.Rigs)
	change(&next)
	tw.reload(&next)
}

// tlAliasSeed adds legacy's seed-tick clear of gc-1's unmanaged alias on
// its live runtime to the identity difference.
func tlAliasSeed() map[string]string {
	return tlMerged(tlIdent(), map[string]string{"provider:SetMeta s-gc-1": "A7 identity", "provider:RemoveMeta s-gc-1": "A7 identity"})
}

// idleTimeline is the idle and drain timers: the idle drain and its latch,
// detached_at, and the INC-003 wake grace.
func idleTimeline() []timelineFixture {
	napping := func() *config.City {
		cfg := tlCity()
		cfg.Agents[0].SleepAfterIdle = "5m"
		return cfg
	}
	// chatNapping adds an always named session "chat" that idle-sleeps after
	// five minutes; named is its row, under the city's session name for it.
	chatNapping := func() *config.City {
		cfg := tlCity()
		cfg.Agents = append(cfg.Agents, config.Agent{Name: "chat", StartCommand: "true", SleepAfterIdle: "5m"})
		cfg.NamedSessions = []config.NamedSession{{Template: "chat", Mode: "always"}}
		return cfg
	}
	chatName := agent.SessionNameFor("test-city", "chat", tlCity().Workspace.SessionTemplate)
	named := chatRow("gc-c", "1", "state", "awake", "session_name", chatName, "instance_token", "tok-gc-c", "last_woke_at", tlAt(-time.Minute), "alias", "chat")
	named.Title = "Display gc-c"
	// slept is legacy's idle sleep of gc-c, its runtime stopped, which v2
	// leaves to A20.
	slept := map[string]string{
		"row:gc-c:*idle": "A20 idle-sleep", "row:gc-c:state": "A20 idle-sleep", "row:gc-c:last_woke_at": "A20 idle-sleep", "runtime:" + chatName: "A20 idle-sleep",
	}
	// Legacy's idle probe succeeds (WaitForIdle answers idle), so it begins
	// and finishes the idle drain itself: the idle sleep, its latch over the
	// always-named demand, and the latch's release once a reload changes the
	// policy (its fingerprint).
	idle := tlFixture("idle, then sleep, then the next pass", chatNapping, []parityRow{named}, nil, []simRuntime{tlLive("gc-c")},
		[]parityStep{
			{Name: "idle past sleep_after_idle", At: 6 * time.Minute},
			{Name: "the next pass", Advance: time.Minute},
			{Name: "the sleep policy changes", Advance: time.Minute, Op: func(tw twin) {
				tw.reconfigure(func(c *config.City) { c.Agents[1].SleepAfterIdle = "10m" })
			}},
		}, tlMerged(tlPol("gc-c"), map[string]string{"row:gc-c:*sync": "A7 row-metadata"}), map[string]map[string]string{
			"idle past sleep_after_idle": tlMerged(slept, map[string]string{
				"provider:SetMeta " + chatName: "A20 idle-sleep", "provider:RemoveMeta " + chatName: "A20 idle-sleep", "provider:Stop " + chatName: "A20 idle-sleep",
				`event:session.stopped chat {"reason":"drain acknowledged","session_id":"gc-c","template":"chat"}`: "A20 idle-sleep",
			}),
			"the next pass":            slept,
			"the sleep policy changes": tlMerged(tlStarted("gc-c", chatName, "chat"), map[string]string{"row:gc-c:*idle": "A20 idle-sleep"}),
		})
	idle.Setup = func(tw twin) {
		tw.sp.mu.Lock()
		tw.sp.WaitForIdleErrors[chatName] = nil
		tw.sp.mu.Unlock()
	}
	attach := tlFixture("attach, then detach, then idle", napping,
		[]parityRow{tlRow("gc-1", 1, time.Minute)}, []parityRow{routedDemandBead("gw-1")}, []simRuntime{{id: "gc-1", epoch: "1", token: "tok-gc-1", attached: true}},
		[]parityStep{
			{Name: "detach", Advance: time.Minute, Op: func(tw twin) {
				tw.sp.SetActivity("s-gc-1", tw.now)
				tw.runtime("gc-1", func(rt simRuntime) *simRuntime { rt.attached = false; return &rt })
			}},
			{Name: "idle past sleep_after_idle", Advance: 6 * time.Minute},
		}, tlMerged(tlPol("gc-1"), tlIdent(), map[string]string{"row:rg-1:*heal": "rig-leg-heal"}), map[string]map[string]string{"seed": tlAliasSeed()})
	// retired is legacy's drain of gc-1, acknowledged and stopped, and the
	// pool slot's close; v2 leaves the drain to A20 and the close to A21.
	retired := map[string]string{
		"row:gc-1:*drain": "A20 drain-begin", "runtime:s-gc-1": "A20 drain-begin", "row:gc-1:*close": "A21 close", "row:gc-1:*sync": "A7 row-metadata",
	}
	// An unwanted row woken a minute ago: legacy keeps it for the 5m wake
	// grace (INC-003), then drains it and retires its slot.
	grace := tlFixture("an unwanted row inside, then past, the wake grace", tlCity,
		[]parityRow{tlRow("gc-1", 1, time.Minute)}, nil, []simRuntime{tlLive("gc-1")},
		[]parityStep{
			{Name: "inside the grace", At: 2 * time.Minute},
			{Name: "past the grace", At: 5 * time.Minute},
			{Name: "past the drain timeout", Advance: 6 * time.Minute},
		}, nil, map[string]map[string]string{
			"past the grace": tlMerged(retired, map[string]string{
				"provider:SetMeta s-gc-1": "A20 drain-begin", "provider:RemoveMeta s-gc-1": "A20 drain-begin", "provider:Stop s-gc-1": "A20 drain-begin",
				`event:session.stopped worker {"reason":"drain acknowledged","session_id":"gc-1","template":"worker"}`: "A20 drain-begin",
			}),
			"past the drain timeout": retired,
		})
	return []timelineFixture{idle, attach, grace}
}
