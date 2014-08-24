package main

import (
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn         = flag.String("dsn", "root@unix(/var/lib/mysql/mysql.sock)/sorttest", "MySQL data source name")
	tableName   = flag.String("table_name", "sorttest", "Name of test table")
	tableSize   = flag.Uint64("table_size", 1000, "Number of records in test table")
	tableEngine = flag.String("table_engine", "InnoDB", "MySQL storage engine")

	// Prepare flags.
	txSize      = flag.Uint64("tx_size", 2000, "How many INSERTs to put in a transaction")
	randomOrder = flag.Bool("random_order", false, "Use ORDER BY RAND() instead of ORDER BY data")

	// Run flags.
	maxTime = flag.Duration("max_time", 10*time.Second, "How long to run the test")
	//maxRequests = flag.Uint64("max_requets", 0, "How many requests to do")
	numThreads = flag.Int("num_threads", 1, "How many threads to use")
)

func panicOnError(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func exec(db *sql.DB, query string) (int64, int64, error) {
	r, err := db.Exec(query)
	if err != nil {
		return 0, 0, err
	}
	last, err := r.LastInsertId()
	panicOnError(err)
	rows, err := r.RowsAffected()
	panicOnError(err)
	return last, rows, nil
}

func numRows(db *sql.DB) uint64 {
	var rows uint64
	q := fmt.Sprintf("SELECT COUNT(*) rows FROM %s", *tableName)
	panicOnError(db.QueryRow(q).Scan(&rows))
	return rows
}

func newTx(db *sql.DB) *sql.Tx {
	tx, err := db.Begin()
	panicOnError(err)
	return tx
}

func randomize(data []byte) string {
	for i := range data {
		data[i] = byte(rand.Int())
	}
	return hex.EncodeToString(data)
}

func prepare(db *sql.DB) {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
data CHAR(120)
) ENGINE=%s`, *tableName, *tableEngine)
	_, _, err := exec(db, q)
	panicOnError(err)

	data := make([]byte, 60)
	start := numRows(db)
	log.Printf("Start number of rows: %v", start)
	tx := newTx(db)
	inserts := *txSize
	for i := start; i < *tableSize; i++ {
		_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s(data) VALUES (?)", *tableName), randomize(data))
		panicOnError(err)
		inserts -= 1
		if inserts == 0 {
			panicOnError(tx.Commit())
			tx = newTx(db)
			inserts = *txSize
		}
	}
	panicOnError(tx.Commit())

	log.Printf("End number of rows: %v", numRows(db))
}

func oneRun(db *sql.DB, done chan int, stop chan bool) {
	log.Println("Start oneRun")
	orderBy := "data"
	if *randomOrder {
		orderBy = "RAND()"
	}
	q := fmt.Sprintf("SELECT SUM(t.id) sum FROM (SELECT id FROM %s ORDER BY %s) AS t", *tableName, orderBy)
	reqs := 0
	for {
		select {
		case <-stop:
			log.Printf("Done %v requests", reqs)
			done <- reqs
			return
		default:
		}
		var sum int
		panicOnError(db.QueryRow(q).Scan(&sum))
		reqs += 1
	}
}

func run(db *sql.DB) {
	done := make(chan int, *numThreads)
	stop := make(chan bool, *numThreads)
	for i := 0; i < *numThreads; i++ {
		go oneRun(db, done, stop)
	}
	log.Printf("Sleep %v", *maxTime)
	time.Sleep(*maxTime)
	for i := 0; i < *numThreads; i++ {
		stop <- true
	}
	total := 0
	for i := 0; i < *numThreads; i++ {
		total = total + <-done
	}
	log.Printf("Total requests: %.2f/s", float64(total)/maxTime.Seconds())
}

func main() {
	flag.Parse()

	if len(flag.Args()) != 1 {
		fmt.Printf("Usage: %s [options] prepare|run|cleanup]\n", os.Args[0])
		return
	}

	db, err := sql.Open("mysql", *dsn)
	panicOnError(err)
	defer db.Close()

	switch flag.Arg(0) {
	case "prepare":
		prepare(db)
	case "run":
		run(db)
	case "cleanup":
		exec(db, fmt.Sprintf("DROP TABLE `%s`", *tableName))
	}
}
