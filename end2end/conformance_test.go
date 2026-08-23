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
	"github.com/dal-go/record"
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

	t.Run("InsertMulti over an existing key fails the whole batch atomically", func(t *testing.T) {
		assertInsertMultiDuplicateKeyIsAtomic(t, db)
	})
}

// insertMultiDuplicateKeyEntity is a minimal payload for
// assertInsertMultiDuplicateKeyIsAtomic; its only requirement is being a
// struct pointer datastore.Put/Mutate can (de)serialize.
type insertMultiDuplicateKeyEntity struct {
	Name string
}

// assertInsertMultiDuplicateKeyIsAtomic exercises the case dalgotest's shared
// conformance suite does not: dalgotest.RunConformance only checks a
// single-record Insert over an existing key (see rejectsDuplicateInsert in
// dalgotest/conformance.go). InsertMulti buffers each record as its own
// Insert mutation on the SAME Datastore transaction (see
// transaction.InsertMulti), so Datastore commits or rejects the whole batch
// as one atomic unit: a duplicate key on one record must fail every record in
// that InsertMulti call, including ones with no conflict of their own — not
// just the colliding record.
func assertInsertMultiDuplicateKeyIsAtomic(t *testing.T, db dal.DB) {
	ctx := context.Background()
	const collection = "dalgo2datastore_insertmulti_duplicate_test"
	const existingID = "existing"
	const newID = "new-alongside-duplicate"

	seedErr := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.Insert(ctx, record.NewRecordWithData(
			record.NewKeyWithID(collection, existingID), &insertMultiDuplicateKeyEntity{Name: existingID}))
	})
	if seedErr != nil {
		t.Fatalf("failed to seed the record to InsertMulti over: %v", seedErr)
	}

	insertErr := db.RunReadwriteTransaction(ctx, func(ctx context.Context, tx dal.ReadwriteTransaction) error {
		return tx.InsertMulti(ctx, []record.Record{
			record.NewRecordWithData(record.NewKeyWithID(collection, newID), &insertMultiDuplicateKeyEntity{Name: newID}),
			record.NewRecordWithData(record.NewKeyWithID(collection, existingID), &insertMultiDuplicateKeyEntity{Name: "duplicate"}),
		})
	})
	if !record.IsAlreadyExists(insertErr) {
		t.Fatalf("InsertMulti over an existing key returned %v, want an error satisfying record.IsAlreadyExists", insertErr)
	}

	getErr := db.RunReadonlyTransaction(ctx, func(ctx context.Context, tx dal.ReadTransaction) error {
		var dst insertMultiDuplicateKeyEntity
		return tx.Get(ctx, record.NewRecordWithData(record.NewKeyWithID(collection, newID), &dst))
	})
	if !record.IsNotFound(getErr) {
		t.Fatalf("the non-conflicting record in a failed InsertMulti batch was persisted anyway (Get returned %v), want record.IsNotFound", getErr)
	}
}
