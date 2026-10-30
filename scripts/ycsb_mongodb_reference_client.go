//go:build ignore

// Separate MongoDB reference control. No TreeDB owner or Go-runtime claims.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type referenceResult struct {
	Attempted bool      `json:"attempted"`
	Success   bool      `json:"success"`
	Error     string    `json:"error"`
	StartUTC  time.Time `json:"start_utc"`
	EndUTC    time.Time `json:"end_utc"`
	ElapsedNS int64     `json:"elapsed_ns"`
}

func referenceCall(fn func() error) referenceResult {
	start := time.Now()
	err := fn()
	end := time.Now()
	r := referenceResult{true, err == nil, "", start.UTC(), end.UTC(), end.Sub(start).Nanoseconds()}
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

type referenceCommand struct {
	Request      json.RawMessage `json:"request"`
	Reply        json.RawMessage `json:"reply"`
	ConnectionID string          `json:"connection_id"`
	Result       referenceResult `json:"result"`
}

func referenceControls(mode string, call func(string, string, bson.D) error) error {
	observe := func(prefix string) error {
		for _, name := range []string{"hello", "buildInfo", "serverStatus", "getCmdLineOpts"} {
			if err := call(prefix+name, name, bson.D{{Key: name, Value: int32(1)}}); err != nil {
				return err
			}
		}
		// Read the actual resolved runtime parameters. Neither image environment
		// nor absent startup overrides establishes these values.
		if err := call(prefix+"getParameter", "getParameter", bson.D{{Key: "getParameter", Value: int32(1)}, {Key: "journalCommitInterval", Value: int32(1)}, {Key: "syncdelay", Value: int32(1)}}); err != nil {
			return err
		}
		return nil
	}
	if err := observe("before."); err != nil {
		return err
	}
	if mode == "checkpoint" {
		if err := call("checkpoint", "fsync", bson.D{{Key: "fsync", Value: int32(1)}, {Key: "lock", Value: false}}); err != nil {
			return err
		}
	}
	return observe("after.")
}

func referenceWrite(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return errors.Join(err, f.Close())
}

func referenceRun() (retErr error) {
	mode := flag.String("mode", "", "observe or checkpoint (explicit fsync lock:false)")
	addr := flag.String("addr", "", "single direct MongoDB reference host:port")
	output := flag.String("observed", "", "exclusive raw canonical Extended JSON command evidence")
	receipt := flag.String("receipt", "", "exclusive checked client cleanup receipt")
	identity := flag.String("fixture-id", "", "bound campaign/generation/control identity")
	flag.Parse()
	if (*mode != "observe" && *mode != "checkpoint") || *addr == "" || *output == "" || *receipt == "" || *identity == "" {
		return errors.New("mode, address, observed, receipt and fixture-id are required")
	}
	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var mu sync.Mutex
	connections := map[string]string{}
	monitor := &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		mu.Lock()
		defer mu.Unlock()
		connections[e.CommandName] = e.ConnectionID
	}}
	// No credentials, URI options, discovery or transparent command retry.
	client, err := mongo.Connect(options.Client().SetHosts([]string{*addr}).SetDirect(true).SetRetryReads(false).SetRetryWrites(false).SetMonitor(monitor))
	if err != nil {
		return err
	}
	commands := map[string]referenceCommand{}
	defer func() {
		// Fresh bounded cleanup context even when the operation context expired.
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		closed := referenceCall(func() error { return client.Disconnect(closeCtx) })
		if !closed.Success {
			retErr = errors.Join(retErr, errors.New(closed.Error))
		}
		status := ""
		if retErr != nil {
			status = retErr.Error()
		}
		e := referenceWrite(*output, map[string]any{"schema_version": 1, "kind": "mongodb_reference_public_commands", "mode": *mode, "address": *addr, "fixture_id": *identity, "commands": commands})
		retErr = errors.Join(retErr, e)
		if e != nil {
			status = retErr.Error()
		}
		observedHash := ""
		if e == nil {
			data, err := os.ReadFile(*output)
			retErr = errors.Join(retErr, err)
			if err != nil {
				status = retErr.Error()
			} else {
				sum := sha256.Sum256(data)
				observedHash = hex.EncodeToString(sum[:])
			}
		}
		retErr = errors.Join(retErr, referenceWrite(*receipt, map[string]any{"schema_version": 1, "kind": "mongodb_reference_control_client_close", "mode": *mode, "fixture_id": *identity, "pid": os.Getpid(), "started_utc": started, "ended_utc": time.Now().UTC(), "result_error": status, "observed_sha256": observedHash, "disconnect": closed}))
	}()
	call := func(key, name string, request bson.D) error {
		requestJSON, err := bson.MarshalExtJSON(request, true, false)
		if err != nil {
			return err
		}
		var raw bson.Raw
		result := referenceCall(func() error {
			var err error
			raw, err = client.Database("admin").RunCommand(ctx, request).Raw()
			return err
		})
		var reply json.RawMessage
		if len(raw) > 0 {
			reply, err = bson.MarshalExtJSON(raw, true, false)
			if err != nil {
				return err
			}
		}
		mu.Lock()
		connection := connections[name]
		mu.Unlock()
		commands[key] = referenceCommand{requestJSON, reply, connection, result}
		if !result.Success {
			return errors.New(result.Error)
		}
		return nil
	}
	return referenceControls(*mode, call)
}

func main() {
	if err := referenceRun(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
