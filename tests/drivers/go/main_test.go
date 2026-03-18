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

// newKeyspaceSession creates a session connected to the test keyspace.
func newKeyspaceSession(t *testing.T) *gocql.Session {
	t.Helper()
	cluster := newCluster()
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("connect to keyspace %s: %v", keyspace, err)
	}
	return session
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

// ---- Collections --------------------------------------------------------

func TestCreateCollectionsTable(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	err := session.Query(`
		CREATE TABLE IF NOT EXISTS collections (
			id int PRIMARY KEY,
			tags list<text>,
			scores set<int>,
			props map<text, text>
		)
	`).Exec()
	if err != nil {
		t.Fatalf("create table collections: %v", err)
	}
}

func TestInsertAndSelectCollections(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	// Insert list
	if err := session.Query(
		"INSERT INTO collections (id, tags) VALUES (?, ?)",
		1, []string{"tag1", "tag2", "tag3"},
	).Exec(); err != nil {
		t.Fatalf("insert list: %v", err)
	}

	// Insert set
	if err := session.Query(
		"INSERT INTO collections (id, scores) VALUES (?, ?)",
		2, []int{10, 20, 30},
	).Exec(); err != nil {
		t.Fatalf("insert set: %v", err)
	}

	// Insert map
	if err := session.Query(
		"INSERT INTO collections (id, props) VALUES (?, ?)",
		3, map[string]string{"key1": "val1", "key2": "val2"},
	).Exec(); err != nil {
		t.Fatalf("insert map: %v", err)
	}

	// Select and verify list
	var tags []string
	if err := session.Query("SELECT tags FROM collections WHERE id = ?", 1).
		Scan(&tags); err != nil {
		t.Fatalf("select list: %v", err)
	}
	if len(tags) != 3 {
		t.Fatalf("expected 3 tags, got %d", len(tags))
	}
	expected := []string{"tag1", "tag2", "tag3"}
	for i, tag := range tags {
		if tag != expected[i] {
			t.Errorf("tags[%d] = %q, want %q", i, tag, expected[i])
		}
	}

	// Select and verify set (gocql returns []int for set<int>)
	var scores []int
	if err := session.Query("SELECT scores FROM collections WHERE id = ?", 2).
		Scan(&scores); err != nil {
		t.Fatalf("select set: %v", err)
	}
	if len(scores) != 3 {
		t.Fatalf("expected 3 scores, got %d", len(scores))
	}
	// Sets are unordered; verify all expected values are present.
	scoreSet := make(map[int]bool)
	for _, s := range scores {
		scoreSet[s] = true
	}
	for _, want := range []int{10, 20, 30} {
		if !scoreSet[want] {
			t.Errorf("expected score %d not found in %v", want, scores)
		}
	}

	// Select and verify map
	var props map[string]string
	if err := session.Query("SELECT props FROM collections WHERE id = ?", 3).
		Scan(&props); err != nil {
		t.Fatalf("select map: %v", err)
	}
	if len(props) != 2 {
		t.Fatalf("expected 2 props, got %d", len(props))
	}
	if props["key1"] != "val1" {
		t.Errorf("props[key1] = %q, want val1", props["key1"])
	}
	if props["key2"] != "val2" {
		t.Errorf("props[key2] = %q, want val2", props["key2"])
	}
}

// ---- ALTER TABLE --------------------------------------------------------

func TestAlterTableAddColumn(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	if err := session.Query("ALTER TABLE users ADD phone text").Exec(); err != nil {
		t.Fatalf("alter table: %v", err)
	}

	if err := session.Query(
		"INSERT INTO users (id, name, phone) VALUES (?, ?, ?)",
		800, "PhoneUser", "555-1234",
	).Exec(); err != nil {
		t.Fatalf("insert with phone: %v", err)
	}

	var name, phone string
	if err := session.Query("SELECT name, phone FROM users WHERE id = ?", 800).
		Scan(&name, &phone); err != nil {
		t.Fatalf("select with phone: %v", err)
	}
	if name != "PhoneUser" {
		t.Errorf("name = %q, want PhoneUser", name)
	}
	if phone != "555-1234" {
		t.Errorf("phone = %q, want 555-1234", phone)
	}
}

// ---- DELETE / UPDATE / LWT ----------------------------------------------

func TestDeleteRow(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	if err := session.Query(
		"INSERT INTO users (id, name) VALUES (?, ?)", 900, "ToDelete",
	).Exec(); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Verify row exists.
	var name string
	if err := session.Query("SELECT name FROM users WHERE id = ?", 900).
		Scan(&name); err != nil {
		t.Fatalf("select before delete: %v", err)
	}

	if err := session.Query("DELETE FROM users WHERE id = ?", 900).Exec(); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify row is gone.
	if err := session.Query("SELECT name FROM users WHERE id = ?", 900).
		Scan(&name); err != gocql.ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got: %v", err)
	}
}

func TestUpdateRow(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	if err := session.Query(
		"INSERT INTO users (id, name, email) VALUES (?, ?, ?)",
		901, "BeforeUpdate", "old@test.com",
	).Exec(); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := session.Query(
		"UPDATE users SET email = ? WHERE id = ?", "new@test.com", 901,
	).Exec(); err != nil {
		t.Fatalf("update: %v", err)
	}

	var email string
	if err := session.Query("SELECT email FROM users WHERE id = ?", 901).
		Scan(&email); err != nil {
		t.Fatalf("select after update: %v", err)
	}
	if email != "new@test.com" {
		t.Errorf("email = %q, want new@test.com", email)
	}
}

func TestInsertIfNotExists(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	// Ensure row does not exist first.
	_ = session.Query("DELETE FROM users WHERE id = ?", 902).Exec()

	// First INSERT IF NOT EXISTS should be applied.
	appliedMap := make(map[string]interface{})
	applied, err := session.Query(
		"INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS",
		902, "LWT",
	).MapScanCAS(appliedMap)
	if err != nil {
		t.Fatalf("first insert IF NOT EXISTS: %v", err)
	}
	if !applied {
		t.Error("first INSERT IF NOT EXISTS should have been applied")
	}

	// Second INSERT IF NOT EXISTS should NOT be applied.
	appliedMap = make(map[string]interface{})
	applied, err = session.Query(
		"INSERT INTO users (id, name) VALUES (?, ?) IF NOT EXISTS",
		902, "LWT2",
	).MapScanCAS(appliedMap)
	if err != nil {
		t.Fatalf("second insert IF NOT EXISTS: %v", err)
	}
	if applied {
		t.Error("second INSERT IF NOT EXISTS should NOT have been applied")
	}
}

// ---- Batch --------------------------------------------------------------

func TestBatchInsert(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query("INSERT INTO users (id, name) VALUES (?, ?)", 701, "Batch1")
	batch.Query("INSERT INTO users (id, name) VALUES (?, ?)", 702, "Batch2")
	batch.Query("INSERT INTO users (id, name) VALUES (?, ?)", 703, "Batch3")

	if err := session.ExecuteBatch(batch); err != nil {
		t.Fatalf("execute batch: %v", err)
	}

	for _, tc := range []struct {
		id   int
		name string
	}{
		{701, "Batch1"},
		{702, "Batch2"},
		{703, "Batch3"},
	} {
		var name string
		if err := session.Query("SELECT name FROM users WHERE id = ?", tc.id).
			Scan(&name); err != nil {
			t.Fatalf("select id=%d: %v", tc.id, err)
		}
		if name != tc.name {
			t.Errorf("id=%d: name = %q, want %q", tc.id, name, tc.name)
		}
	}
}

// ---- TTL ----------------------------------------------------------------

func TestInsertWithTTL(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	if err := session.Query(
		"INSERT INTO users (id, name) VALUES (?, ?) USING TTL 1",
		950, "Ephemeral",
	).Exec(); err != nil {
		t.Fatalf("insert with TTL: %v", err)
	}

	// Verify row exists immediately.
	var name string
	if err := session.Query("SELECT name FROM users WHERE id = ?", 950).
		Scan(&name); err != nil {
		t.Fatalf("select right after insert: %v", err)
	}
	if name != "Ephemeral" {
		t.Errorf("name = %q, want Ephemeral", name)
	}

	// Wait for TTL to expire.
	time.Sleep(2 * time.Second)

	if err := session.Query("SELECT name FROM users WHERE id = ?", 950).
		Scan(&name); err != gocql.ErrNotFound {
		t.Errorf("expected ErrNotFound after TTL expiry, got: %v (name=%q)", err, name)
	}
}

// ---- LIMIT / COUNT ------------------------------------------------------

func TestSelectCount(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	var count int
	if err := session.Query("SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		t.Fatalf("select count: %v", err)
	}
	if count <= 0 {
		t.Errorf("expected count > 0, got %d", count)
	}
	t.Logf("users count = %d", count)
}

func TestSelectLimit(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	// Ensure at least 3 rows exist so LIMIT 2 is meaningful.
	for i, n := range []string{"Limit1", "Limit2", "Limit3"} {
		if err := session.Query(
			"INSERT INTO users (id, name) VALUES (?, ?)", 601+i, n,
		).Exec(); err != nil {
			t.Fatalf("insert limit row %d: %v", i, err)
		}
	}

	iter := session.Query("SELECT id FROM users LIMIT 2").Iter()
	count := 0
	var id int
	for iter.Scan(&id) {
		count++
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("select limit: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows with LIMIT 2, got %d", count)
	}
}

// ---- Error handling -----------------------------------------------------

func TestQueryNonexistentTable(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	err := session.Query("SELECT * FROM nonexistent_table_xyz").Exec()
	if err == nil {
		t.Error("expected error querying nonexistent table, got nil")
	}
	t.Logf("expected error: %v", err)
}

func TestInvalidSyntax(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	err := session.Query("SELEC BROKEN QUERY").Exec()
	if err == nil {
		t.Error("expected error for invalid syntax, got nil")
	}
	t.Logf("expected error: %v", err)
}

// ---- system_schema introspection ----------------------------------------

func TestSystemSchemaKeyspaces(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	iter := session.Query("SELECT keyspace_name FROM system_schema.keyspaces").Iter()
	var ksName string
	found := false
	for iter.Scan(&ksName) {
		if ksName == keyspace {
			found = true
		}
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("query system_schema.keyspaces: %v", err)
	}
	if !found {
		t.Errorf("keyspace %q not found in system_schema.keyspaces", keyspace)
	}
}

// ---- NULL handling ------------------------------------------------------

func TestNullHandling(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	// Insert with nil name.
	if err := session.Query(
		"INSERT INTO users (id, name) VALUES (?, ?)", 960, nil,
	).Exec(); err != nil {
		t.Fatalf("insert null: %v", err)
	}

	// Select and verify null is returned.
	var name *string
	if err := session.Query("SELECT name FROM users WHERE id = ?", 960).
		Scan(&name); err != nil {
		t.Fatalf("select null: %v", err)
	}
	if name != nil {
		t.Errorf("expected nil name, got %q", *name)
	}
}

// ---- Secondary index ----------------------------------------------------

func TestCreateIndex(t *testing.T) {
	session := newKeyspaceSession(t)
	defer session.Close()

	err := session.Query("CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)").Exec()
	if err != nil {
		t.Fatalf("create index: %v", err)
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
