package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/rogpeppe/fastuuid"
	"go.uber.org/zap"
)

// Config for bank workload
type Config struct {
	NumAccounts         int           
	Interval            time.Duration 
	Concurrency         int           
	RetryLimit          int           
	Contention          string        
	DbName              string
	TiFlashDataReplicas int
	MinLength           int
	MaxLength           int
}

// Default configuration
var defaultConfig = Config{
	NumAccounts:   1000,
	Interval:      time.Second * 2,
	Concurrency:   50,
	RetryLimit:    10,
	Contention:    "low",
	DbName:        "test",
	MinLength:     0,
	MaxLength:     1024,
}

// Bank2Client implements the bank workload
type Bank2Client struct {
	*Config
	wg    sync.WaitGroup
	stop  int32
	txnID int32
}

const (
	initialBalance    = 1000
	insertConcurrency = 100
	insertBatchSize   = 100
	maxTransfer       = 100
	systemAccountID   = 0
)

var (
	stmtsCreate = []string{
		`CREATE TABLE IF NOT EXISTS bank2_accounts (
		id INT,
		balance INT NOT NULL,
		name VARCHAR(32),
		remark VARCHAR(2048),
		PRIMARY KEY (id),
		UNIQUE INDEX byName (name),
		KEY byBalance (balance)
	);`,
		`CREATE TABLE IF NOT EXISTS bank2_transaction (
		id INT,
		booking_date TIMESTAMP DEFAULT NOW(),
		txn_date TIMESTAMP DEFAULT NOW(),
		txn_ref VARCHAR(32),
		remark VARCHAR(2048),
		PRIMARY KEY (id),
		UNIQUE INDEX byTxnRef (txn_ref)
	);`,
		`CREATE TABLE IF NOT EXISTS bank2_transaction_leg (
		id INT AUTO_INCREMENT,
		account_id INT,
		amount INT NOT NULL,
		running_balance INT NOT NULL,
		txn_id INT,
		remark VARCHAR(2048),
		PRIMARY KEY (id)
	);`,
		`TRUNCATE TABLE bank2_accounts;`,
		`TRUNCATE TABLE bank2_transaction;`,
		`TRUNCATE TABLE bank2_transaction_leg;`,
	}
	stmtsTruncate = []string{
		`TRUNCATE TABLE bank2_transaction;`,
		`TRUNCATE TABLE bank2_transaction_leg;`,
	}
	tableNames = []string{
		"bank2_accounts",
		"bank2_transaction",
		"bank2_transaction_leg",
	}
	remark = strings.Repeat("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXVZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXVZlkjsanksqiszndqpijdslnnq", 16)

	// PadLength returns a random padding length for `remark` field within user
	// specified bound.
	baseLen      = [3]int{36, 48, 16}
	tidbDatabase = true
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// RunBank starts the bank workload
func RunBank(ctx context.Context, dsn string, runTime time.Duration, cfg Config) error {
	log.Info("Start bank workload")

	bclient := &Bank2Client{Config: &cfg}
	
	// Setup phase
	err := bclient.SetUp(ctx, dsn)
	if err != nil {
		return fmt.Errorf("setup failed: %w", err)
	}

	// Start the workload
	go func() {
		err := bclient.Start(ctx, dsn)
		if err != nil {
			log.Warn("bank workload start failed", zap.Error(err))
		}
	}()

	// Start verification in parallel
	go func() {
		err := bclient.ContinueVerify(ctx, dsn)
		if err != nil {
			log.Warn("verify failed", zap.Error(err))
		}
	}()

	// Wait for specified runtime
	if runTime > 0 {
		time.Sleep(runTime)
		log.Info("Runtime completed, stopping workload")
		bclient.TearDown(ctx)
	}

	return nil
}

func (c *Bank2Client) padLength(table int) int {
	// Default values
	minLen := 0
	maxLen := len(remark) / 2 // Use half the remark length as default max
	
	if table >= 0 && table < 3 {
		// If MinLength and MaxLength are properly set in config, use them
		if c.Config.MinLength > 0 && c.Config.MinLength > baseLen[table] {
			minLen = c.Config.MinLength - baseLen[table]
		}
		
		if c.Config.MaxLength > 0 && c.Config.MaxLength < len(remark)+baseLen[table] {
			maxLen = c.Config.MaxLength - baseLen[table]
		}
	}
	
	// Make sure maxLen is greater than minLen
	if maxLen <= minLen {
		maxLen = minLen + 100
	}
	
	// Make sure we don't go out of bounds for the remark string
	if minLen >= len(remark) {
		return len(remark) - 1
	}
	
	// Ensure we don't cause panic with random number generation
	diff := maxLen - minLen
	if diff <= 0 {
		diff = 1
	}
	
	return minLen + rand.Intn(diff)
}

// SetUp with only once account verify after finished.
func (c *Bank2Client) SetUp(ctx context.Context, dsn string) error {
	log.Info("Setting up bank tables and accounts...")
	db, err := OpenDB(dsn, 1)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create tables
	for _, stmt := range stmtsCreate {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	// Insert accounts in batches
	var wg sync.WaitGroup
	type Job struct {
		begin, end int
	}

	ch := make(chan Job)
	for i := 0; i < insertConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range ch {
				select {
				case <-ctx.Done():
					return
				default:
				}
				args := make([]string, 0, insertBatchSize)
				for i := job.begin; i < job.end; i++ {
					args = append(args, fmt.Sprintf(`(%d, %d, "account %d", "%s")`, i, initialBalance, i, remark[:c.padLength(0)]))
				}

				query := fmt.Sprintf("INSERT IGNORE INTO bank2_accounts (id, balance, name, remark) VALUES %s", strings.Join(args, ","))
				_, err := db.Exec(query)
				if err != nil && !strings.Contains(err.Error(), "Duplicate entry") {
					log.Error("Insert accounts error", zap.Error(err))
				}
			}
		}()
	}

	// Create jobs
	var begin, end int
	for begin = 1; begin <= c.Config.NumAccounts; begin = end {
		end = begin + insertBatchSize
		if end > c.Config.NumAccounts {
			end = c.Config.NumAccounts + 1
		}
		select {
		case <-ctx.Done():
			return nil
		case ch <- Job{begin: begin, end: end}:
		}
	}
	close(ch)
	wg.Wait()

	// Insert system account
	query := fmt.Sprintf(`INSERT IGNORE INTO bank2_accounts (id, balance, name) VALUES (%d, %d, "system account")`, 
		systemAccountID, int64(c.Config.NumAccounts*initialBalance))
	_, err = db.Exec(query)
	if err != nil && !strings.Contains(err.Error(), "Duplicate entry") {
		return err
	}

	// Verify setup
	ret, err := c.Verify(ctx, db)
	if err != nil {
		return err
	}
	if !ret {
		return fmt.Errorf("initial balance verification failed")
	}
	
	return nil
}

func (c *Bank2Client) TearDown(ctx context.Context) error {
	atomic.StoreInt32(&c.stop, 1)
	return nil
}

// Start with bank2_transaction and bank2_transaction_leg truncated firstly
func (c *Bank2Client) Start(ctx context.Context, dsn string) error {
	atomic.StoreInt32(&c.stop, 0)
	log.Info("Starting bank transfers...")
	
	db, err := OpenDB(dsn, c.Config.Concurrency)
	if err != nil {
		return err
	}
	defer db.Close()

	// Clear transaction tables
	for _, stmt := range stmtsTruncate {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < c.Config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if atomic.LoadInt32(&c.stop) != 0 {
						return
					}
					
					c.wg.Add(1)
					c.moveMoney(ctx, db)
					c.wg.Done()
					
					// Small delay to prevent CPU overload
					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}
	
	wg.Wait()
	return nil
}

// ContinueVerify periodically checks account balances
func (c *Bank2Client) ContinueVerify(ctx context.Context, dsn string) error {
	db, err := OpenDB(dsn, 1)
	if err != nil {
		return err
	}
	defer db.Close()

	// Initial verification
	if ok, err := c.Verify(ctx, db); err != nil || !ok {
		if err != nil {
			return err
		}
		return fmt.Errorf("initial verification failed")
	}

	// Continuous verification
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(c.Config.Interval):
			if atomic.LoadInt32(&c.stop) != 0 {
				return nil
			}
			
			ok, err := c.Verify(ctx, db)
			if err != nil {
				log.Error("Verification error", zap.Error(err))
			} else if !ok {
				log.Error("Verification failed, accounts are inconsistent")
				atomic.StoreInt32(&c.stop, 1)
				return fmt.Errorf("verification failed")
			}
		}
	}
}

// Verify checks if the sum of all account balances is correct
func (c *Bank2Client) Verify(ctx context.Context, db *sql.DB, sessionParams ...string) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// Get TiDB timestamp if using TiDB
	if tidbDatabase {
		var tso uint64
		if err = tx.QueryRow("SELECT @@tidb_current_ts").Scan(&tso); err == nil {
			log.Info("SELECT SUM(balance) to verify use tso",
				zap.Any("client", c),
				zap.Uint64("tso", tso))
		}
	}

	// Set isolation read engines if needed
	_, err = tx.Exec("set @@session.tidb_isolation_read_engines='tikv'")
	if err != nil {
		log.Warn("Failed to set isolation read engines", zap.Error(err))
	}

	// Apply any session parameters
	for _, param := range sessionParams {
		if _, err = tx.Exec(param); err != nil {
			log.Warn("Failed to set session parameter", zap.Error(err))
			return false, err
		}
	}

	var total int64
	expectTotal := (int64(c.Config.NumAccounts) * initialBalance) * 2

	// Verification query
	uuid := fastuuid.MustNewGenerator().Hex128()
	query := fmt.Sprintf("SELECT SUM(balance) AS total, '%s' as uuid FROM bank2_accounts", uuid)
	if err = tx.QueryRow(query).Scan(&total, &uuid); err != nil {
		return false, err
	}
	
	if total != expectTotal {
		log.Error("Balance verification failed",
			zap.Int64("expected", expectTotal),
			zap.Int64("actual", total),
			zap.String("uuid", uuid))
		atomic.StoreInt32(&c.stop, 1)
		c.wg.Wait()
		return false, nil
	}
	
	log.Info("Balance verification passed", zap.Int64("total", total))
	return true, nil
}

func (c *Bank2Client) moveMoney(ctx context.Context, db *sql.DB) {
	from, to := rand.Intn(c.Config.NumAccounts), rand.Intn(c.Config.NumAccounts)
	if c.Config.Contention == "high" {
		if from > c.Config.NumAccounts/2 {
			from = systemAccountID
		} else {
			to = systemAccountID
		}
	}
	
	if from == to {
		return
	}
	
	amount := rand.Intn(maxTransfer)
	if err := c.execTransaction(ctx, db, from, to, amount); err != nil {
		log.Debug("Transaction failed", zap.Error(err))
	}
}

func (c *Bank2Client) execTransaction(ctx context.Context, db *sql.DB, from, to int, amount int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Lock the rows
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(
		"SELECT id, balance FROM bank2_accounts WHERE id IN (%d, %d) FOR UPDATE", from, to))
	if err != nil {
		return err
	}
	defer rows.Close()

	var fromBalance, toBalance int
	balances := make(map[int]int)
	
	for rows.Next() {
		var id, balance int
		if err := rows.Scan(&id, &balance); err != nil {
			return err
		}
		balances[id] = balance
	}
	
	if err := rows.Err(); err != nil {
		return err
	}
	
	// Check if we found both accounts
	if len(balances) != 2 {
		return fmt.Errorf("expected 2 accounts, got %d", len(balances))
	}
	
	fromBalance = balances[from]
	toBalance = balances[to]
	
	// Check sufficient funds
	if fromBalance < amount {
		return nil // Insufficient funds, not an error
	}

	// Update balances
	txnID := atomic.AddInt32(&c.txnID, 1)
	
	// Record transaction with remark
	remarkPad1 := ""
	if len(remark) > 0 {
		remarkLen := c.padLength(1)
		if remarkLen < len(remark) {
			remarkPad1 = remark[:remarkLen]
		} else {
			remarkPad1 = remark
		}
	}
	
	if _, err := tx.Exec(
		"INSERT INTO bank2_transaction (id, txn_ref, remark) VALUES (?, ?, ?)",
		txnID, fmt.Sprintf("txn_%d", txnID), remarkPad1); err != nil {
		return err
	}
	
	// Get remark for transaction legs
	remarkPad2 := ""
	if len(remark) > 0 {
		remarkLen := c.padLength(2)
		if remarkLen < len(remark) {
			remarkPad2 = remark[:remarkLen]
		} else {
			remarkPad2 = remark
		}
	}
	
	// Record from leg
	if _, err := tx.Exec(
		"INSERT INTO bank2_transaction_leg (account_id, amount, running_balance, txn_id, remark) VALUES (?, ?, ?, ?, ?)",
		from, -amount, fromBalance-amount, txnID, remarkPad2); err != nil {
		return err
	}
	
	// Record to leg
	if _, err := tx.Exec(
		"INSERT INTO bank2_transaction_leg (account_id, amount, running_balance, txn_id, remark) VALUES (?, ?, ?, ?, ?)",
		to, amount, toBalance+amount, txnID, remarkPad2); err != nil {
		return err
	}
	
	// Update accounts
	if _, err := tx.Exec(
		"UPDATE bank2_accounts SET balance = ? WHERE id = ?",
		fromBalance-amount, from); err != nil {
		return err
	}
	
	if _, err := tx.Exec(
		"UPDATE bank2_accounts SET balance = ? WHERE id = ?",
		toBalance+amount, to); err != nil {
		return err
	}

	return tx.Commit()
}

// OpenDB opens a database specified by its database driver name and a
// driver-specific data source name.
func OpenDB(dsn string, maxConns int) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(time.Minute * 10)

	return db, nil
}

// RunWithRetry runs a function with retry logic
func RunWithRetry(ctx context.Context, retryLimit int, interval time.Duration, f func() error) error {
	var err error
	for i := 0; i < retryLimit; i++ {
		err = f()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(interval):
		}
	}
	return err
}

// IsErrDupEntry returns true if error is a duplicate entry error
func IsErrDupEntry(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Duplicate entry")
}

// String return client name
func (c *Bank2Client) String() string {
	return "bank2"
}

func main() {
	// Define command line flags
	var (
		dsn         string
		numAccounts int
		concurrency int
		duration    time.Duration
		interval    time.Duration
		contention  string
	)

	flag.StringVar(&dsn, "dsn", "root:@tcp(127.0.0.1:4000)/test", "Database connection string")
	flag.IntVar(&numAccounts, "accounts", 1000, "Number of bank accounts")
	flag.IntVar(&concurrency, "concurrency", 50, "Number of concurrent workers")
	flag.DurationVar(&duration, "duration", 10*time.Minute, "Test duration")
	flag.DurationVar(&interval, "interval", 2*time.Second, "Verification interval")
	flag.StringVar(&contention, "contention", "low", "Contention level: low, high")
	flag.Parse()

	// Initialize config
	cfg := defaultConfig
	cfg.NumAccounts = numAccounts
	cfg.Concurrency = concurrency
	cfg.Interval = interval
	cfg.Contention = contention

	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run the bank workload
	err := RunBank(ctx, dsn, duration, cfg)
	if err != nil {
		log.Fatal("Bank workload failed", zap.Error(err))
	}
}