// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// tsnet-loadtest creates n+1 tsnet instances to test memory usage and socket buffers.
// One instance acts as a client connecting to n server instances.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"tailscale.com/tsnet"
)

var (
	centralAuthKey  = flag.String("central-authkey", "", "Auth key for central client instance (required)")
	loadtestAuthKey = flag.String("loadtest-authkey", "", "Auth key for loadtest server instances (required)")
	stateDir        = flag.String("statedir", "", "Base state directory (required)")
	n               = flag.Int("n", 1, "Number of server instances")
	oneshot         = flag.Bool("oneshot", false, "Exit after all servers read 1 byte, dumping heap profile to stdout")
)

var serversReadCount atomic.Int32

// Track all server instances for dehydration
var (
	serversMu sync.Mutex
	servers   []*tsnet.Server
)

func logf(format string, args ...any) {
	now := time.Now().Format("2006-01-02 15:04:05.000")
	log.Printf("[%s] "+format, append([]any{now}, args...)...)
}

func main() {
	flag.Parse()

	if *centralAuthKey == "" {
		log.Fatal("--central-authkey is required")
	}
	if *loadtestAuthKey == "" {
		log.Fatal("--loadtest-authkey is required")
	}
	if *stateDir == "" {
		log.Fatal("--statedir is required")
	}
	if *n < 1 {
		log.Fatal("--n must be at least 1")
	}

	if err := os.MkdirAll(*stateDir, 0700); err != nil {
		log.Fatalf("Failed to create statedir: %v", err)
	}

	// Start pprof HTTP server
	go func() {
		logf("Starting pprof server on http://0.0.0.0:6060/debug/pprof/")
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatalf("Failed to start pprof server: %v", err)
		}
	}()

	ctx := context.Background()

	// Create n server instances
	serverAddrs := make([]string, *n)
	servers = make([]*tsnet.Server, *n)
	for i := 0; i < *n; i++ {
		idx := i
		go func() {
			srv := &tsnet.Server{
				Hostname:  fmt.Sprintf("loadtest-server-%d", idx),
				AuthKey:   *loadtestAuthKey,
				Ephemeral: true,
				Dir:       filepath.Join(*stateDir, fmt.Sprintf("server-%d", idx)),
			}

			// Track server for dehydration
			serversMu.Lock()
			servers[idx] = srv
			serversMu.Unlock()

			logf("Starting server instance %d", idx)
			if err := srv.Start(); err != nil {
				log.Fatalf("Server %d failed to start: %v", idx, err)
			}

			st, err := srv.Up(ctx)
			if err != nil {
				log.Fatalf("Server %d failed to come up: %v", idx, err)
			}
			serverAddrs[idx] = st.TailscaleIPs[0].String()
			logf("Server %d up: %s", idx, serverAddrs[idx])

			ln, err := srv.Listen("tcp", ":12345")
			if err != nil {
				log.Fatalf("Server %d failed to listen: %v", idx, err)
			}
			logf("Server %d listening on port 12345", idx)

			for {
				conn, err := ln.Accept()
				if err != nil {
					log.Fatalf("Server %d accept failed: %v", idx, err)
				}
				logf("Server %d accepted connection from %s", idx, conn.RemoteAddr())

				go func(c net.Conn) {
					buf := make([]byte, 1)
					_, err := io.ReadFull(c, buf)
					if err != nil {
						log.Printf("Server %d read failed: %v", idx, err)
						return
					}
					logf("Server %d read 1 byte", idx)

					// Track reads for oneshot mode
					if *oneshot {
						count := serversReadCount.Add(1)
						if int(count) == *n {
							logf("All %d servers have read 1 byte", *n)
							doOneshotDehydrateTest()
						}
					}

					// Pause forever
					<-make(chan struct{})
				}(conn)
			}
		}()
	}

	// Wait for all servers to be ready
	logf("Waiting for all servers to be ready...")
	for {
		ready := true
		for i := 0; i < *n; i++ {
			if serverAddrs[i] == "" {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	logf("All servers ready")

	// Create client instance
	client := &tsnet.Server{
		Hostname:  "loadtest-client",
		AuthKey:   *centralAuthKey,
		Ephemeral: true,
		Dir:       filepath.Join(*stateDir, "client"),
	}

	logf("Starting client instance")
	if err := client.Start(); err != nil {
		log.Fatalf("Client failed to start: %v", err)
	}

	_, err := client.Up(ctx)
	if err != nil {
		log.Fatalf("Client failed to come up: %v", err)
	}
	logf("Client up")

	// Connect to each server and send data
	for i := 0; i < *n; i++ {
		idx := i
		go func() {
			addr := net.JoinHostPort(serverAddrs[idx], "12345")
			logf("Client connecting to server %d at %s", idx, addr)

			conn, err := client.Dial(ctx, "tcp", addr)
			if err != nil {
				log.Fatalf("Client failed to connect to server %d: %v", idx, err)
			}
			logf("Client connected to server %d", idx)

			// Write 1000000 nul bytes
			data := make([]byte, 1000000)
			n, err := conn.Write(data)
			if err != nil {
				log.Fatalf("Client failed to write to server %d: %v", idx, err)
			}
			logf("Client wrote %d bytes to server %d", n, idx)

			// Close write direction
			if tc, ok := conn.(*net.TCPConn); ok {
				tc.CloseWrite()
			}

			// Pause forever
			<-make(chan struct{})
		}()
	}

	// Pause forever
	logf("Setup complete, pausing forever")
	<-make(chan struct{})
}

// doOneshotDehydrateTest performs the dehydration test for oneshot mode:
// wait 5s, GC twice, save before.pprof, dehydrate all servers, GC twice, write pprof to stdout.
func doOneshotDehydrateTest() {
	// logf("Waiting 5 seconds before dehydration test...")
	// time.Sleep(5 * time.Second)

	logf("Running GC twice (before dehydrate)...")
	runtime.GC()
	runtime.GC()

	logf("Writing heap profile to before.pprof...")
	f, err := os.Create("before.pprof")
	if err != nil {
		log.Fatalf("Failed to create before.pprof: %v", err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatalf("Failed to write before.pprof: %v", err)
	}
	f.Close()

	logf("Dehydrating all %d server instances...", len(servers))
	serversMu.Lock()
	for i, srv := range servers {
		if srv == nil {
			continue
		}
		logf("Dehydrating server %d...", i)
		if err := srv.Dehydrate(); err != nil {
			log.Printf("Failed to dehydrate server %d: %v", i, err)
		}
	}
	serversMu.Unlock()
	logf("All servers dehydrated")

	logf("Running GC twice (after dehydrate)...")
	runtime.GC()
	runtime.GC()

	logf("Writing heap profile to stdout...")
	if err := pprof.WriteHeapProfile(os.Stdout); err != nil {
		log.Fatalf("Failed to write heap profile to stdout: %v", err)
	}

	logf("Dehydration test complete, exiting")
	os.Exit(0)
}
