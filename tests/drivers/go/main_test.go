// Package main provides CQL driver smoke tests using gocql.
//
// Each test is idempotent and uses the "go_test" keyspace.
package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

const keyspace = "go_test"

func ferrosaHost() string {
	if h := os.Getenv("FERROSA_HOST"); h != "" {
		return h
	}
	return "127.0.0.1"
}

func ferrosaPort() int {
	if p := os.Getenv("FERROSA_CQL_PORT"); p != "" {
		var port int
		fmt.Sscanf(p, "%d", &port)
		if port > 0 {
			return port
		}
	}
	return 9042
}

// newCluster returns a gocql ClusterConfig pointed at Ferrosa.
func newCluster() *gocql.ClusterConfig {
	cluster := gocql.NewCluster(ferrosaHost())
	cluster.Port = ferrosaPort()
	cluster.ProtoVersion = 5
	cluster.Consistency = gocql.One
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	return cluster
}

// ---- Connection & introspection -----------------------------------------

func TestConnect(t *testing.T) {
	cluster := newCluster()
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer session.Close()
}

func TestSystemLocal(t *testing.T) {
	cluster := newCluster()
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	var clusterName, dc string
	if err := session.Query("SELECT cluster_name, data_center FROM system.local").
		Scan(&clusterName, &dc); err != nil {
		t.Fatalf("query system.local: %v", err)
	}
	if clusterName == "" {
		t.Error("cluster_name is empty")
	}
	t.Logf("cluster_name=%s  data_center=%s", clusterName, dc)
}

func TestSystemPeers(t *testing.T) {
	cluster := newCluster()
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	iter := session.Query("SELECT * FROM system.peers").Iter()
	// Single-node may have 0 peers; just verify no error.
	iter.Close()
}

// ---- DDL ----------------------------------------------------------------

func TestCreateKeyspace(t *testing.T) {
	cluster := newCluster()
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	err = session.Query(
		`CREATE KEYSPACE IF NOT EXISTS ` + keyspace + ` WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`).
		Exec()
	if err != nil {
		t.Fatalf("create keyspace: %v", err)
	}
}

func TestCreateTable(t *testing.T) {
	cluster := newCluster()
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	err = session.Query(`
		CREATE TABLE IF NOT EXISTS users (
			id int PRIMARY KEY,
			name text,
			email text,
			active boolean,
			score float,
			rating double,
			age bigint
		)
	`).Exec()
	if err != nil {
		t.Fatalf("create table users: %v", err)
	}

	err = session.Query(`
		CREATE TABLE IF NOT EXISTS events (
			user_id int,
			ts timestamp,
			data text,
			PRIMARY KEY (user_id, ts)
		)
	`).Exec()
	if err != nil {
		t.Fatalf("create table events: %v", err)
	}
}

// ---- DML ----------------------------------------------------------------

func TestInsertAndSelect(t *testing.T) {
	cluster := newCluster()
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	// Insert
	if err := session.Query(
		"INSERT INTO users (id, name, email, active, score, rating, age) VALUES (?, ?, ?, ?, ?, ?, ?)",
		1, "Alice", "alice@test.com", true, float32(95.5), 99.12345678, int64(9223372036854775807),
	).Exec(); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Select
	var name, email string
	var active bool
	var score float32
	var rating float64
	var age int64

	if err := session.Query("SELECT name, email, active, score, rating, age FROM users WHERE id = ?", 1).
		Scan(&name, &email, &active, &score, &rating, &age); err != nil {
		t.Fatalf("select: %v", err)
	}

	if name != "Alice" {
		t.Errorf("name = %q, want Alice", name)
	}
	if email != "alice@test.com" {
		t.Errorf("email = %q, want alice@test.com", email)
	}
	if !active {
		t.Error("active should be true")
	}
	if age != 9223372036854775807 {
		t.Errorf("age = %d, want max int64", age)
	}
}

func TestClusteringInsertAndRange(t *testing.T) {
	cluster := newCluster()
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

	if err := session.Query("INSERT INTO events (user_id, ts, data) VALUES (?, ?, ?)",
		1, ts1, "login").Exec(); err != nil {
		t.Fatalf("insert event 1: %v", err)
	}
	if err := session.Query("INSERT INTO events (user_id, ts, data) VALUES (?, ?, ?)",
		1, ts2, "logout").Exec(); err != nil {
		t.Fatalf("insert event 2: %v", err)
	}

	iter := session.Query("SELECT data FROM events WHERE user_id = ? ORDER BY ts ASC", 1).Iter()
	var data string
	var results []string
	for iter.Scan(&data) {
		results = append(results, data)
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("select events: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 events, got %d", len(results))
	}
	if results[0] != "login" {
		t.Errorf("first event = %q, want login", results[0])
	}
	if results[1] != "logout" {
		t.Errorf("second event = %q, want logout", results[1])
	}
}

// ---- Prepared statements ------------------------------------------------

func TestPreparedStatement(t *testing.T) {
	cluster := newCluster()
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	// Prepare insert
	if err := session.Query(
		"INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
		100, "GoPrepared", "go-prepared@test.com",
	).Exec(); err != nil {
		t.Fatalf("prepared insert: %v", err)
	}

	// Prepare select
	var name string
	if err := session.Query("SELECT name FROM users WHERE id = ?", 100).
		Scan(&name); err != nil {
		t.Fatalf("prepared select: %v", err)
	}
	if name != "GoPrepared" {
		t.Errorf("name = %q, want GoPrepared", name)
	}
}

// ---- Cleanup ------------------------------------------------------------

func TestZZDropKeyspace(t *testing.T) {
	cluster := newCluster()
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer session.Close()

	if err := session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec(); err != nil {
		t.Fatalf("drop keyspace: %v", err)
	}
}
