package session

import (
	"fmt"
	"runtime/debug"

	"github.com/gastownhall/gascity/internal/beads"
)

// GuardSessionKeys makes every store write of a session-row key the registry
// does not have call fail with the key and its stack, whoever made the write:
// production code, a fixture, a test helper. A fixture that needs a key no
// production code writes spells it test_* (the registry's fixture-only
// family). Every test that writes a session row is then a census of the keys
// written, computed ones included. Tests install it (cmd/gc, internal/api,
// internal/session, internal/worker); production never does.
func GuardSessionKeys(fail func(msg string)) {
	beads.SetSessionKeyHook(func(key string) {
		if _, ok := LookupField(key); !ok {
			fail(fmt.Sprintf("session row key %q is not in the session field registry (internal/session/fields.go)\n%s", key, debug.Stack()))
		}
	})
}
