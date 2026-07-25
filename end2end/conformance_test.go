package end2end

import (
	"context"
	"io"
	"log"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dalgotest"
	"github.com/dal-go/dalgo2datastore"
)

// TestConformance runs the shared dalgotest suite against a live Datastore
// emulator, reusing the same emulator-lifecycle helpers as TestEndToEnd.
//
// This is the regression test for dalgo2datastore's own package-level Put/
// PutMulti function variables (see put.go): those bypass dal.Record and
// dal.DB entirely and are a known, unvalidated write path this suite cannot
// reach. What this suite does prove is that every write dal.NewDB can see —
// Insert/Set/UpdateRecord and their Multi variants through a transaction —
// is validated uniformly, including UpdateRecord, whose adapter-level Update
// always answers dal.ErrNotSupported: validation runs before that answer is
// ever reached, so an invalid record is still rejected.
func TestConformance(t *testing.T) {
	log.Println("TestConformance() started...")
	cmd, cmdStdOutput, cmdErrOutput := startDatastoreEmulator(t)
	defer terminateConformanceEmulator(t, cmd)
	emulatorExited := false
	go handleCommandOutput(t, "error", cmdStdOutput, &emulatorExited)
	go handleCommandOutput(t, "standard", cmdErrOutput, &emulatorExited)
	select {
	case <-handleEmulatorClosing(t, cmd):
		emulatorExited = true
	case <-waitForEmulatorReadiness(&emulatorExited):
		if !emulatorExited {
			runConformanceAgainstEmulator(t)
			emulatorExited = true
			time.Sleep(time.Second)
		}
	}
	time.Sleep(10 * time.Millisecond)
}

// terminateConformanceEmulator stops the emulator process this test started.
//
// It deliberately does not reuse terminateDatastoreEmulator: that helper's
// nil check is inverted (it returns unless cmd is nil, so its shutdown code
// never runs for a real process) and calls t.Errorf on a successful shutdown
// response, failing the test on the happy path. That is harmless in CI's
// ephemeral runners — the process dies with the runner regardless — but can
// leave a "java ... CloudDatastore" process running in a persistent local/dev
// environment, same as TestEndToEnd already can. Fixing the shared helper is
// out of scope here, so this test manages its own shutdown instead: the same
// graceful /shutdown request, logged rather than failed on success, then Kill
// of the direct child as a best-effort fallback. This repo's kill-ports.sh
// remains the documented manual recourse (`kill -9 $(lsof -ti:8081,8747)`)
// when a grandchild JVM outlives its parent; a test deliberately killing
// whatever else happens to hold a port is its own hazard and is not done
// here.
func terminateConformanceEmulator(t *testing.T, cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if resp, err := http.Get("http://localhost:8081/shutdown"); err != nil {
		t.Logf("Datastore emulator shutdown request failed: %v", err)
	} else {
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			t.Logf("failed to read Datastore emulator shutdown response: %v", readErr)
		} else {
			t.Logf("Datastore emulator shutdown response: %s", string(body))
		}
	}
	time.Sleep(2 * time.Second)
	_ = cmd.Process.Kill()
	time.Sleep(1 * time.Second)
}

func runConformanceAgainstEmulator(t *testing.T) {
	setDatastoreEnvVars()

	db, err := dalgo2datastore.NewDatabase(context.Background(), gCloudProjectID)
	if err != nil {
		t.Fatalf("failed to create datastore client: %v", err)
	}

	dalgotest.RunConformance(t, func(t *testing.T) (dal.DB, func()) {
		return db, nil
	})
}
