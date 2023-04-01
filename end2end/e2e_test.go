package end2end

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/strongo/dalgo/end2end"
	"github.com/strongo/dalgo2datastore"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestEndToEnd(t *testing.T) {
	log.Println("TestEndToEnd() started...")
	cmd, _, cmdStdErr := startDatastoreEmulator(t)
	defer terminateDatastoreEmulator(t, cmd)
	defer func() {
		err := recover()
		if err != nil {
			t.Errorf("Panic: %v", err)
			terminateDatastoreEmulator(t, cmd)
		}
	}()
	emulatorExited := false
	go handleCommandStderr(t, cmdStdErr, &emulatorExited)
	select {
	case <-handleEmulatorClosing(t, cmd):
		emulatorExited = true
	case <-waitForEmulatorReadiness(&emulatorExited):
		if !emulatorExited {
			testEndToEnd(t)
			emulatorExited = true
			time.Sleep(time.Second)
		}
	}
	time.Sleep(10 * time.Millisecond)
}

func handleCommandStderr(t *testing.T, stderr *bytes.Buffer, emulatorExited *bool) {
	var s string
	for {
		if *emulatorExited && s != "" {
			t.Log("STDERR from Datastore emulator:\t" + s)
			return
		}
		line, err := stderr.ReadString('\n')
		if line != "" {
			s += line
		}
		if err != nil {
			if err == io.EOF {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			t.Errorf("Failed to read from Datastore emulator STDERR: %v", err)
			return
		}
	}
}

func terminateDatastoreEmulator(t *testing.T, cmd *exec.Cmd) {
	if cmd != nil {
		return
	}
	if resp, err := http.Get("http://localhost:8081/shutdown"); err != nil {
		t.Error("Failed to shutdown Datastore emulator:", err)
	} else {
		defer func() {
			_ = resp.Body.Close()
		}()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Error("Failed to read response from shutdown Datastore emulator:", err)
		}
		t.Errorf("Datastore emulator shutdown response: %s", string(body))
	}
	time.Sleep(2 * time.Second)
	_ = cmd.Process.Kill()
	time.Sleep(1 * time.Second)
}

const gCloudProjectID = "dalgo"

func startDatastoreEmulator(t *testing.T) (cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
	stdout = new(bytes.Buffer)
	stderr = new(bytes.Buffer)

	// If port is busy run in terminal: kill -9 $(lsof -ti:8081)

	cmd = exec.Command("gcloud", "beta", "emulators", "datastore", "start", "--project", gCloudProjectID)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	//cmd.Env = []string{"CLOUDSDK_CORE_PROJECT=" + gCloudProjectID}

	t.Log("Starting Datastore emulator...")
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start Datastore emulator: %v", err)
	}
	return
}

func setDatastoreEnvVars() {
	const emulatorHost = "localhost:8081"
	vars := map[string]string{
		"CLOUDSDK_CORE_PROJECT":        gCloudProjectID,
		"GAE_APPLICATION":              gCloudProjectID,
		"GOOGLE_CLOUD_PROJECT":         gCloudProjectID,
		"DATASTORE_DATASET":            gCloudProjectID,
		"DATASTORE_PROJECT_ID":         gCloudProjectID,
		"DATASTORE_EMULATOR_HOST":      emulatorHost,
		"DATASTORE_EMULATOR_HOST_PATH": emulatorHost + "/datastore",
		"DATASTORE_HOST":               "http://" + emulatorHost,
	}
	for k, v := range vars {
		if err := os.Setenv(k, v); err != nil {
			panic(fmt.Sprintf("Failed to set env variable %s=%s: %v", k, v, err))
		}
	}
}

func waitForEmulatorReadiness(emulatorExited *bool) (emulatorIsReady chan bool) {
	emulatorIsReady = make(chan bool)
	time.Sleep(time.Second)
	go func() {
		for {
			_, err := http.Get("http://localhost:8081/") // On separate line for debug purposes
			if err == nil || *emulatorExited {
				emulatorIsReady <- true
				close(emulatorIsReady)
				break
			}
			time.Sleep(9 * time.Millisecond)
		}
	}()
	return
}

func handleEmulatorClosing(t *testing.T, cmd *exec.Cmd) (emulatorErrors chan error) {
	emulatorErrors = make(chan error)
	go func() {
		err := cmd.Wait() // Intentionally not in IF statement
		if err != nil {
			if err.Error() == "signal: killed" {
				t.Log("Datastore emulator killed.")
			} else {
				t.Error("Datastore emulator failed:", err)
				emulatorErrors <- err
			}
		} else {
			t.Log("Datastore emulator completed.")
		}
		close(emulatorErrors)
	}()
	return
}

func testEndToEnd(t *testing.T) {
	//if err := os.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:8080"); err != nil {
	//	t.Fatalf("Failed to set env variable FIRESTORE_EMULATOR_HOST: %v", err)
	//}

	setDatastoreEnvVars()

	db, err := dalgo2datastore.NewDatabase(context.Background(), "dalgo")
	if err != nil {
		t.Fatalf("Failed to create datastore client: %v", err)
	}
	assert.NotNil(t, db)
	end2end.TestDalgoDB(t, db)
}
