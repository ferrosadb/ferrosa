// Ferrosa-Jepsen workload generator — Go driver.
//
// Connects to a Ferrosa/Cassandra cluster via CQL and runs register, bank,
// or LWT workload patterns, recording operation history as JSONL.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gocql/gocql"
)

const (
	numAccounts    = 10
	initialBalance = 1000
)

// Operation matches the Rust Operation struct for JSONL output.
type Operation struct {
	ClientID   string      `json:"client_id"`
	InvokeUS   int64       `json:"invoke_us"`
	CompleteUS int64       `json:"complete_us"`
	Op         interface{} `json:"op"`
	Result     interface{} `json:"result"`
}

func nowUS() int64 {
	return time.Now().UnixMicro()
}

// writeOp writes one JSONL line to the file.
func writeOp(f *os.File, op Operation) {
	data, err := json.Marshal(op)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal error: %v\n", err)
		return
	}
	f.Write(data)
	f.WriteString("\n")
}

func resultFromErr(err error) interface{} {
	if err == nil {
		return "Ok"
	}
	s := err.Error()
	if strings.Contains(strings.ToLower(s), "timeout") {
		return "Timeout"
	}
	return map[string]string{"Err": s}
}

// ---------------------------------------------------------------------------
// Schema setup
// ---------------------------------------------------------------------------

const createKS = "CREATE KEYSPACE IF NOT EXISTS jepsen " +
	"WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"

func setupRegister(session *gocql.Session) error {
	if err := session.Query(createKS).Exec(); err != nil {
		return err
	}
	if err := session.Query(
		"CREATE TABLE IF NOT EXISTS jepsen.register (id int PRIMARY KEY, val int)",
	).Exec(); err != nil {
		return err
	}
	return session.Query("INSERT INTO jepsen.register (id, val) VALUES (0, 0)").Exec()
}

func setupBank(session *gocql.Session) error {
	if err := session.Query(createKS).Exec(); err != nil {
		return err
	}
	if err := session.Query(
		"CREATE TABLE IF NOT EXISTS jepsen.accounts (id int PRIMARY KEY, balance bigint)",
	).Exec(); err != nil {
		return err
	}
	for i := 0; i < numAccounts; i++ {
		if err := session.Query(
			fmt.Sprintf("INSERT INTO jepsen.accounts (id, balance) VALUES (%d, %d)", i, initialBalance),
		).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func setupLWT(session *gocql.Session, num int) error {
	if err := session.Query(createKS).Exec(); err != nil {
		return err
	}
	return session.Query(
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS jepsen.lwt%d (id text PRIMARY KEY, val text)", num),
	).Exec()
}

// ---------------------------------------------------------------------------
// Workload runners
// ---------------------------------------------------------------------------

func runRegister(session *gocql.Session, f *os.File, clientID string, dur time.Duration, ctx context.Context) {
	deadline := time.Now().Add(dur)
	counter := int64(1)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r := rand.Float64()
		if r < 0.5 {
			// Read
			op := map[string]interface{}{"Read": map[string]string{"key": "0"}}
			invoke := nowUS()
			var val *int64
			var readVal int64
			err := session.Query("SELECT val FROM jepsen.register WHERE id = 0").Scan(&readVal)
			complete := nowUS()
			var result interface{}
			if err != nil && err != gocql.ErrNotFound {
				result = resultFromErr(err)
			} else if err == gocql.ErrNotFound {
				result = map[string]interface{}{"Value": nil}
			} else {
				val = &readVal
				result = map[string]interface{}{"Value": *val}
			}
			writeOp(f, Operation{clientID, invoke, complete, op, result})

		} else if r < 0.8 {
			// Write
			op := map[string]interface{}{"Write": map[string]interface{}{"key": "0", "value": counter}}
			invoke := nowUS()
			err := session.Query(
				fmt.Sprintf("UPDATE jepsen.register SET val = %d WHERE id = 0", counter),
			).Exec()
			complete := nowUS()
			writeOp(f, Operation{clientID, invoke, complete, op, resultFromErr(err)})
			counter++

		} else {
			// CAS
			expected := counter - 1
			op := map[string]interface{}{"Cas": map[string]interface{}{
				"key": "0", "expected": expected, "value": counter,
			}}
			invoke := nowUS()
			var applied bool
			err := session.Query(
				fmt.Sprintf("UPDATE jepsen.register SET val = %d WHERE id = 0 IF val = %d", counter, expected),
			).Scan(&applied)
			complete := nowUS()
			var result interface{}
			if err != nil && err != gocql.ErrNotFound {
				result = resultFromErr(err)
			} else {
				result = map[string]bool{"Applied": applied}
			}
			writeOp(f, Operation{clientID, invoke, complete, op, result})
			counter++
		}
	}
}

func runBank(session *gocql.Session, f *os.File, clientID string, dur time.Duration, ctx context.Context) {
	deadline := time.Now().Add(dur)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		r := rand.Float64()
		if r < 0.7 {
			fromID := rand.Intn(numAccounts)
			toID := rand.Intn(numAccounts)
			if toID == fromID {
				toID = (fromID + 1) % numAccounts
			}
			amount := int64(rand.Intn(100) + 1)

			// Read source balance
			op := map[string]interface{}{"Read": map[string]string{"key": fmt.Sprintf("account-%d", fromID)}}
			invoke := nowUS()
			var balance int64
			err := session.Query(
				fmt.Sprintf("SELECT balance FROM jepsen.accounts WHERE id = %d", fromID),
			).Scan(&balance)
			complete := nowUS()
			if err != nil {
				writeOp(f, Operation{clientID, invoke, complete, op, resultFromErr(err)})
				continue
			}
			writeOp(f, Operation{clientID, invoke, complete, op, map[string]interface{}{"Value": balance}})

			if balance < amount {
				continue
			}

			// CAS debit
			newBalance := balance - amount
			casOp := map[string]interface{}{"Cas": map[string]interface{}{
				"key": fmt.Sprintf("account-%d", fromID), "expected": balance, "value": newBalance,
			}}
			invoke = nowUS()
			var applied bool
			err = session.Query(
				fmt.Sprintf("UPDATE jepsen.accounts SET balance = %d WHERE id = %d IF balance = %d",
					newBalance, fromID, balance),
			).Scan(&applied)
			complete = nowUS()
			if err != nil && err != gocql.ErrNotFound {
				writeOp(f, Operation{clientID, invoke, complete, casOp, resultFromErr(err)})
				continue
			}
			writeOp(f, Operation{clientID, invoke, complete, casOp, map[string]bool{"Applied": applied}})
			if !applied {
				continue
			}

			// Credit destination
			creditOp := map[string]interface{}{"Write": map[string]interface{}{
				"key": fmt.Sprintf("account-%d", toID), "value": amount,
			}}
			invoke = nowUS()
			err = session.Query(
				fmt.Sprintf("UPDATE jepsen.accounts SET balance = balance + %d WHERE id = %d", amount, toID),
			).Exec()
			complete = nowUS()
			writeOp(f, Operation{clientID, invoke, complete, creditOp, resultFromErr(err)})

		} else {
			// Read all balances
			op := map[string]interface{}{"SerialRead": map[string]string{"key": "all-accounts"}}
			invoke := nowUS()
			values := make([][]string, 0, numAccounts)
			hadError := false
			for i := 0; i < numAccounts; i++ {
				var bal int64
				err := session.Query(
					fmt.Sprintf("SELECT balance FROM jepsen.accounts WHERE id = %d", i),
				).Scan(&bal)
				if err != nil {
					complete := nowUS()
					writeOp(f, Operation{clientID, invoke, complete, op, resultFromErr(err)})
					hadError = true
					break
				}
				values = append(values, []string{fmt.Sprintf("account-%d", i), fmt.Sprintf("%d", bal)})
			}
			if !hadError {
				complete := nowUS()
				writeOp(f, Operation{clientID, invoke, complete, op, map[string]interface{}{"CurrentValues": values}})
			}
		}
	}
}

func runLWT(session *gocql.Session, f *os.File, clientID string, dur time.Duration, patternNum int, ctx context.Context) {
	table := fmt.Sprintf("jepsen.lwt%d", patternNum)
	deadline := time.Now().Add(dur)
	seq := 0

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if patternNum == 1 || patternNum == 4 || patternNum == 8 {
			// INSERT IF NOT EXISTS
			val := fmt.Sprintf("v%d", seq)
			op := map[string]interface{}{"InsertIfNotExists": map[string]interface{}{
				"table": table, "pk": "pk-0",
				"values": [][]string{{"val", val}},
			}}
			invoke := nowUS()
			var applied bool
			err := session.Query(
				fmt.Sprintf("INSERT INTO %s (id, val) VALUES ('pk-0', '%s') IF NOT EXISTS", table, val),
			).Scan(&applied)
			complete := nowUS()
			var result interface{}
			if err != nil && err != gocql.ErrNotFound {
				result = resultFromErr(err)
			} else {
				result = map[string]bool{"Applied": applied}
			}
			writeOp(f, Operation{clientID, invoke, complete, op, result})

		} else if patternNum == 3 {
			// DELETE IF
			op := map[string]interface{}{"DeleteIf": map[string]interface{}{
				"table": table, "pk": "pk-0", "condition": "val IS NOT NULL",
			}}
			invoke := nowUS()
			var applied bool
			err := session.Query(
				fmt.Sprintf("DELETE FROM %s WHERE id = 'pk-0' IF EXISTS", table),
			).Scan(&applied)
			complete := nowUS()
			var result interface{}
			if err != nil && err != gocql.ErrNotFound {
				result = resultFromErr(err)
			} else {
				result = map[string]bool{"Applied": applied}
			}
			writeOp(f, Operation{clientID, invoke, complete, op, result})

		} else {
			// UPDATE IF (default for most patterns)
			expected := seq
			newVal := seq + 1
			op := map[string]interface{}{"UpdateIf": map[string]interface{}{
				"table": table, "pk": "pk-0",
				"condition":   fmt.Sprintf("val = %d", expected),
				"assignments": [][]string{{"val", fmt.Sprintf("%d", newVal)}},
			}}
			invoke := nowUS()
			var applied bool
			err := session.Query(
				fmt.Sprintf("UPDATE %s SET val = '%d' WHERE id = 'pk-0' IF val = '%d'", table, newVal, expected),
			).Scan(&applied)
			complete := nowUS()
			var result interface{}
			if err != nil && err != gocql.ErrNotFound {
				result = resultFromErr(err)
			} else {
				result = map[string]bool{"Applied": applied}
				if applied {
					seq = newVal
				}
			}
			writeOp(f, Operation{clientID, invoke, complete, op, result})
		}
		seq++
	}
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	contactPoints := flag.String("contact-points", "", "Comma-separated contact points")
	workload := flag.String("workload", "", "Workload name")
	duration := flag.Int("duration", 60, "Duration in seconds")
	threads := flag.Int("threads", 4, "Number of goroutines")
	outputDir := flag.String("output-dir", "", "Output directory for JSONL")
	clientIDFlag := flag.String("client-id", "go", "Client ID prefix")
	flag.Parse()

	if *contactPoints == "" || *workload == "" || *outputDir == "" {
		fmt.Fprintln(os.Stderr, "Required: --contact-points, --workload, --output-dir")
		os.Exit(1)
	}

	hosts := strings.Split(*contactPoints, ",")
	for i := range hosts {
		hosts[i] = strings.TrimSpace(hosts[i])
	}

	if err := os.MkdirAll(*outputDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir: %v\n", err)
		os.Exit(1)
	}

	// Create cluster for setup
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = 30 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}

	patternNum := 0
	switch {
	case *workload == "register":
		if err := setupRegister(session); err != nil {
			fmt.Fprintf(os.Stderr, "setup: %v\n", err)
			os.Exit(1)
		}
	case *workload == "bank":
		if err := setupBank(session); err != nil {
			fmt.Fprintf(os.Stderr, "setup: %v\n", err)
			os.Exit(1)
		}
	case strings.HasPrefix(*workload, "lwt-"):
		parts := strings.Split(*workload, "-")
		if len(parts) < 2 {
			fmt.Fprintf(os.Stderr, "invalid LWT workload: %s\n", *workload)
			os.Exit(1)
		}
		fmt.Sscanf(parts[1], "%d", &patternNum)
		if err := setupLWT(session, patternNum); err != nil {
			fmt.Fprintf(os.Stderr, "setup: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown workload: %s\n", *workload)
		os.Exit(1)
	}
	session.Close()

	// Signal handling
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigCh
		cancel()
	}()

	dur := time.Duration(*duration) * time.Second
	var wg sync.WaitGroup

	for i := 0; i < *threads; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			tid := fmt.Sprintf("%s-%d", *clientIDFlag, idx)

			wCluster := gocql.NewCluster(hosts...)
			wCluster.Timeout = 10 * time.Second
			wSession, err := wCluster.CreateSession()
			if err != nil {
				fmt.Fprintf(os.Stderr, "worker connect: %v\n", err)
				return
			}
			defer wSession.Close()

			path := filepath.Join(*outputDir, tid+".jsonl")
			f, err := os.Create(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "create file: %v\n", err)
				return
			}
			defer f.Close()

			switch {
			case *workload == "register":
				runRegister(wSession, f, tid, dur, ctx)
			case *workload == "bank":
				runBank(wSession, f, tid, dur, ctx)
			default:
				runLWT(wSession, f, tid, dur, patternNum, ctx)
			}
		}(i)
	}

	wg.Wait()
}
