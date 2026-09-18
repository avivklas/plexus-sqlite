package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/avivklas/plexus"
	"github.com/avivklas/plexus-sqlite/pkg/pgwire"
	"github.com/avivklas/plexus-sqlite/pkg/sqlstore"
)

func main() {
	var (
		nodeID    = flag.String("node-id", "node-1", "Unique node identifier in cluster")
		bindAddr  = flag.String("bind-addr", "127.0.0.1:9000", "Cluster Raft and RPC listen address")
		advAddr   = flag.String("advertise-addr", "", "Address advertised to cluster peers for Raft and RPC (e.g. 172.31.x.x:9000)")
		pgAddr    = flag.String("pg-addr", "127.0.0.1:5432", "PostgreSQL wire protocol listen address")
		pprofAddr = flag.String("pprof-addr", "", "HTTP pprof profile address (e.g. 127.0.0.1:6060)")
		dataDir   = flag.String("data-dir", "./data", "Directory for Raft consensus log store")
		inMemory  = flag.Bool("in-memory", true, "Open SQLite database in memory (default: true)")
		bootstrap = flag.Bool("bootstrap", false, "Bootstrap this node as the initial cluster leader")
		joinAddrs = flag.String("join", "", "Comma-separated list of peer RPC addresses to join")
		syncLog      = flag.Bool("sync-log", false, "Synchronously fsync Raft log appends to disk (default: false)")
		followerWait = flag.Bool("follower-wait", true, "Wait for follower local FSM apply on writes before returning ACK (default: true)")
	)
	flag.Parse()

	if *pprofAddr != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
		go func() {
			log.Printf("Starting pprof HTTP server on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	log.Printf("Starting Distributed SQLite Server (Plexus-SQLite)...")
	log.Printf("Node ID: %s", *nodeID)
	log.Printf("Cluster Addr: %s", *bindAddr)
	if *advAddr != "" {
		log.Printf("Advertise Addr: %s", *advAddr)
	}
	log.Printf("Postgres Protocol Addr: %s", *pgAddr)
	log.Printf("Consensus Data Dir: %s", *dataDir)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	var (
		sqlStore *sqlstore.Store
		err      error
	)
	if *inMemory {
		log.Printf("SQLite Storage: In-Memory (shared cache)")
		sqlStore, err = sqlstore.NewInMemory()
	} else {
		dbPath := filepath.Join(*dataDir, "sqlite.db")
		log.Printf("SQLite Storage: On-Disk (%s)", dbPath)
		sqlStore, err = sqlstore.New(dbPath)
	}
	if err != nil {
		log.Fatalf("failed to initialize sqlite store: %v", err)
	}
	defer sqlStore.Close()

	// Configure cluster
	clusterCfg := plexus.DefaultClusterConfig(*nodeID, *bindAddr, *dataDir)
	clusterCfg.Bootstrap = *bootstrap
	clusterCfg.ApplyTimeout = 10 * time.Second
	clusterCfg.SyncLog = *syncLog
	clusterCfg.FollowerWaitLocalApply = *followerWait
	if *advAddr != "" {
		clusterCfg.AdvertiseAddr = *advAddr
	}

	if *joinAddrs != "" {
		for _, addr := range strings.Split(*joinAddrs, ",") {
			trimmed := strings.TrimSpace(addr)
			if trimmed != "" {
				clusterCfg.JoinAddrs = append(clusterCfg.JoinAddrs, trimmed)
			}
		}
	}

	cluster, err := plexus.NewCluster(clusterCfg)
	if err != nil {
		log.Fatalf("failed to initialize plexus cluster: %v", err)
	}

	// Register sqlstore with cluster via UseState
	mutator := cluster.UseState(sqlStore)
	sqlStore.AttachMutator(mutator)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start cluster consensus
	if err := cluster.Start(ctx); err != nil {
		log.Fatalf("failed to start cluster: %v", err)
	}
	defer cluster.Stop()

	// Start PostgreSQL wire protocol server
	pgServer, err := pgwire.NewServer(*pgAddr, sqlStore)
	if err != nil {
		log.Fatalf("failed to start pgwire server on %s: %v", *pgAddr, err)
	}
	defer pgServer.Close()

	log.Printf("Plexus-SQLite ready! Connect via standard postgres clients:")
	log.Printf("  psql -h %s -p %s -U postgres\n", strings.Split(*pgAddr, ":")[0], strings.Split(*pgAddr, ":")[1])

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down Plexus-SQLite server...")
}
