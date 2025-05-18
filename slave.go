package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

var master net.Conn
var connected bool
var db *sql.DB
var localDbName string
var replicationInProgress bool
var dbUser, dbPassword string

func setupLocalDB(dbName string) error {
	// Configure connection
	cfg := mysql.NewConfig()
	cfg.User = dbUser
	cfg.Passwd = dbPassword

	// First connect without specifying a database
	var err error
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("connection error: %v", err)
	}

	// Check if we can connect
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("failed to connect to database server: %v", err)
	}

	// Create the database if it doesn't exist
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + dbName)
	if err != nil {
		return fmt.Errorf("error creating database: %v", err)
	}

	// Now connect to the specific database
	db.Close()
	cfg.DBName = dbName
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("connection error: %v", err)
	}

	// Verify we can connect to the database
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	localDbName = dbName
	return nil
}

func connectToMaster(addr string) bool {
	var err error
	master, err = net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Failed to connect to master at %s: %v\n", addr, err)
		return false
	}

	fmt.Println("Connected to master server!")
	connected = true

	// Listen for messages from master in a goroutine
	go listenToMaster()
	return true
}

func executeLocalQuery(query string) error {
	if db == nil {
		return fmt.Errorf("local database connection not established")
	}

	// For debugging
	if strings.HasPrefix(strings.ToUpper(query), "CREATE TABLE") {
		fmt.Printf("Executing CREATE TABLE query: %s\n", query)
	}

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("local query execution error: %v", err)
	}
	return nil
}

// Handle a CREATE TABLE statement with special error handling
func executeCreateTable(query string) error {
	if db == nil {
		return fmt.Errorf("local database connection not established")
	}

	fmt.Printf("Executing CREATE TABLE query: %s\n", query)

	// Basic validation
	if !strings.HasPrefix(strings.ToUpper(query), "CREATE TABLE") {
		return fmt.Errorf("invalid CREATE TABLE statement: %s", query)
	}

	// Clean up any potential issues in the query
	// Remove any backticks that might cause problems
	cleanQuery := strings.ReplaceAll(query, "`", "")

	// Execute the CREATE TABLE statement
	_, err := db.Exec(cleanQuery)
	if err != nil {
		// If there's an error, try to get more specific error details
		fmt.Printf("Error details for CREATE TABLE: %v\n", err)
		return fmt.Errorf("local query execution error: %v", err)
	}

	// Verify the table was created
	tableName := ""
	// Extract table name from CREATE TABLE statement
	matches := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)`).FindStringSubmatch(query)
	if len(matches) >= 2 {
		tableName = matches[1]
		fmt.Printf("Extracted table name: %s\n", tableName)

		// Check if table exists
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
			localDbName, tableName).Scan(&count)
		if err != nil {
			fmt.Printf("Failed to verify table creation: %v\n", err)
		} else if count == 0 {
			return fmt.Errorf("table %s was not created successfully", tableName)
		} else {
			fmt.Printf("Table %s verified as existing in database\n", tableName)
		}
	}

	return nil
}

func listenToMaster() {
	defer func() {
		master.Close()
		connected = false
		fmt.Println("Disconnected from master server.")
	}()

	scanner := bufio.NewScanner(master)
	// Increase scanner buffer size to handle larger CREATE TABLE statements
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		message := scanner.Text()
		parts := strings.SplitN(message, ":", 2)
		if len(parts) != 2 {
			fmt.Println("Received malformed message from master")
			continue
		}

		msgType := parts[0]
		content := parts[1]

		switch msgType {
		case "init_replication":
			fmt.Printf("\nInitializing replication for database: %s\n", content)
			replicationInProgress = true

			// Setup local database for replication
			err := setupLocalDB(content)
			if err != nil {
				fmt.Printf("Failed to setup local database: %v\n", err)
				replicationInProgress = false
			} else {
				fmt.Printf("Local database '%s' ready for replication\n", content)
			}

		case "create_db":
			fmt.Printf("Creating database: %s\n", content)
			if !replicationInProgress && db == nil {
				fmt.Println("Replication not in progress and no local database, ignoring create_db command")
				continue
			}

			// Database already created in setupLocalDB or we try to create it now
			if db == nil {
				err := setupLocalDB(content)
				if err != nil {
					fmt.Printf("Failed to create database: %v\n", err)
				}
			}

		case "create_table":
			fmt.Println("Creating table from master schema")

			// Check if we have a valid CREATE TABLE statement
			if !strings.HasPrefix(strings.ToUpper(content), "CREATE TABLE") {
				fmt.Printf("Invalid CREATE TABLE statement received: %s\n", content)
				continue
			}

			// Use specialized function for CREATE TABLE
			err := executeCreateTable(content)
			if err != nil {
				fmt.Printf("Failed to create table: %v\n", err)
				fmt.Printf("SQL statement was: %s\n", content)
				continue
			}
			fmt.Println("Table created successfully in local database")

		case "sync_data":
			// Always process data sync commands, even if not in replication mode
			// This allows for adding data to tables that were created after initial replication
			err := executeLocalQuery(content)
			if err != nil {
				fmt.Printf("Failed to sync data: %v\n", err)
				// Check for specific errors like missing tables
				if strings.Contains(err.Error(), "Error 1146") {
					fmt.Println("Table doesn't exist for this data. Request schema from master.")
					// Try to extract table name from INSERT statement
					if strings.HasPrefix(strings.ToUpper(content), "INSERT INTO") {
						parts := strings.Fields(content)
						if len(parts) >= 3 {
							tableName := strings.TrimSpace(parts[2])
							// Remove any trailing characters like ( or spaces
							tableName = strings.Split(tableName, "(")[0]
							fmt.Printf("Requesting schema for table '%s'\n", tableName)
							fmt.Fprintf(master, "get_table_schema:%s\n", tableName)
						}
					}
				}
				continue
			}

		case "replication_complete":
			replicationInProgress = false
			fmt.Println("Initial replication completed successfully!")

		case "replicate_query":
			fmt.Println("Applying replicated query to local database")
			err := executeLocalQuery(content)
			if err != nil {
				fmt.Printf("Failed to execute replicated query: %v\n", err)
				fmt.Printf("Query was: %s\n", content)

				// Special handling for missing table errors
				if strings.Contains(err.Error(), "Error 1146") && strings.Contains(err.Error(), "doesn't exist") {
					// Extract table name from error
					errParts := strings.Split(err.Error(), "'")
					if len(errParts) >= 4 {
						tableName := strings.Split(errParts[3], ".")[1]
						fmt.Printf("Table '%s' doesn't exist. Requesting schema from master...\n", tableName)

						// Request table schema from master
						fmt.Fprintf(master, "get_table_schema:%s\n", tableName)
					}
				}
				continue
			}
			fmt.Println("Query applied successfully to local database")

		case "verification_data":
			if content == "begin" {
				fmt.Println("\nReceiving verification data from master:")

				// Process table info
				masterTables := make(map[string]int)

				// Read table information
				for scanner.Scan() {
					tableInfo := scanner.Text()

					// Check if verification data is complete
					if tableInfo == "verification_data:end" {
						break
					}

					// Parse table info (format: "table:name:count")
					infoParts := strings.Split(tableInfo, ":")
					if len(infoParts) != 3 || infoParts[0] != "table" {
						fmt.Printf("Invalid table info format: %s\n", tableInfo)
						continue
					}

					tableName := infoParts[1]
					tableCount := 0
					fmt.Sscanf(infoParts[2], "%d", &tableCount)

					masterTables[tableName] = tableCount
					fmt.Printf("  - Master table: %s: %d rows\n", tableName, tableCount)
				}

				// Compare with local tables
				compareReplication(masterTables)
			}

		case "drop_database":
			fmt.Printf("Dropping local database '%s'\n", content)
			if db != nil {
				_, err := db.Exec("DROP DATABASE IF EXISTS " + content)
				if err != nil {
					fmt.Printf("Error dropping database: %v\n", err)
				} else {
					fmt.Println("Local database dropped successfully")
					db.Close()
					db = nil
					localDbName = ""
				}
			}

		case "notification":
			fmt.Printf("\n--- Master notification: %s ---\n", content)

		case "success":
			if content == "query executed" {
				fmt.Println("Query executed successfully on master")
			} else {
				// It's a select result with column count
				var columnCount int
				numCols, err := fmt.Sscanf(content, "%d", &columnCount)
				if err != nil || numCols != 1 {
					fmt.Println("Invalid column count received")
					continue
				}

				// Get column names
				if !scanner.Scan() {
					fmt.Println("Failed to read column names")
					break
				}
				columns := scanner.Text()
				fmt.Printf("\n%s\n", columns)
				fmt.Println(strings.Repeat("-", len(columns)*2))

				// Display rows
				rowCount := 0
				for scanner.Scan() {
					row := scanner.Text()
					if row == "END" {
						break
					}
					rowCount++
					fmt.Println(row)
				}
				fmt.Printf("Total rows: %d\n", rowCount)
			}

		case "error":
			fmt.Printf("Error from master: %s\n", content)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Scanner error: %v\n", err)
	}
}

// Compare local replication with master tables
func compareReplication(masterTables map[string]int) {
	if db == nil {
		fmt.Println("Local database not available")
		return
	}

	// Get local tables and counts
	localTables := make(map[string]int)
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		fmt.Printf("Error getting local tables: %v\n", err)
		return
	}

	var tableName string
	for rows.Next() {
		rows.Scan(&tableName)

		// Count rows in this table
		var rowCount int
		err := db.QueryRow("SELECT COUNT(*) FROM " + tableName).Scan(&rowCount)
		if err != nil {
			fmt.Printf("Error counting rows in %s: %v\n", tableName, err)
			continue
		}

		localTables[tableName] = rowCount
	}
	rows.Close()

	// Compare tables
	fmt.Println("\n=== REPLICATION VERIFICATION RESULTS ===")

	allMatch := true

	// Check for master tables that should be in local
	for masterTable, masterCount := range masterTables {
		localCount, exists := localTables[masterTable]

		if !exists {
			fmt.Printf("MISSING: Table '%s' exists on master but not locally\n", masterTable)
			allMatch = false
			continue
		}

		if localCount != masterCount {
			fmt.Printf("MISMATCH: Table '%s' has %d rows locally but %d rows on master\n",
				masterTable, localCount, masterCount)
			allMatch = false
		} else {
			fmt.Printf("MATCH: Table '%s' has %d rows on both master and locally\n",
				masterTable, localCount)
		}
	}

	// Check for local tables not in master
	for localTable := range localTables {
		_, exists := masterTables[localTable]
		if !exists {
			fmt.Printf("EXTRA: Table '%s' exists locally but not on master\n", localTable)
			allMatch = false
		}
	}

	if allMatch {
		fmt.Println("\nReplication status: SYNCHRONIZED ✓")
	} else {
		fmt.Println("\nReplication status: OUT OF SYNC ✗")
	}
}

func sendQuery(operation, query string) {
	if !connected {
		fmt.Println("Not connected to master server")
		return
	}

	request := fmt.Sprintf("%s:%s\n", operation, query)
	_, err := fmt.Fprint(master, request)
	if err != nil {
		fmt.Printf("Failed to send query to master: %v\n", err)
		connected = false
		return
	}
}

func insertRecord() {
	var tableName string
	fmt.Print("Enter table name: ")
	fmt.Scanln(&tableName)

	fmt.Println("Enter column names and values separated by equals sign (name=value), one per line")
	fmt.Println("Enter empty line when done")

	columns := []string{}
	values := []string{}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "" {
			break
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			fmt.Println("Invalid format. Use column=value")
			continue
		}

		columns = append(columns, parts[0])
		values = append(values, "'"+parts[1]+"'") // Note: simple quoting, not safe for all values
	}

	if len(columns) == 0 {
		fmt.Println("No data provided")
		return
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(values, ", "))

	sendQuery("insert", query)
}

func updateRecord() {
	var tableName string
	fmt.Print("Enter table name: ")
	fmt.Scanln(&tableName)

	var id string
	fmt.Print("Enter ID of record to update: ")
	fmt.Scanln(&id)

	fmt.Println("Enter column names and values to update separated by equals sign (name=value), one per line")
	fmt.Println("Enter empty line when done")

	updates := []string{}

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "" {
			break
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			fmt.Println("Invalid format. Use column=value")
			continue
		}

		updates = append(updates, fmt.Sprintf("%s = '%s'", parts[0], parts[1]))
	}

	if len(updates) == 0 {
		fmt.Println("No updates provided")
		return
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = %s",
		tableName,
		strings.Join(updates, ", "),
		id)

	sendQuery("update", query)
}

func deleteRecord() {
	var tableName string
	fmt.Print("Enter table name: ")
	fmt.Scanln(&tableName)

	var id string
	fmt.Print("Enter ID of record to delete: ")
	fmt.Scanln(&id)

	query := fmt.Sprintf("DELETE FROM %s WHERE id = %s", tableName, id)
	sendQuery("delete", query)
}

func selectRecords() {
	var query string
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Enter SELECT query:")
	fmt.Print("> ")
	query, _ = reader.ReadString('\n')
	query = strings.TrimSpace(query)

	if !strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		fmt.Println("Query must start with SELECT")
		return
	}

	sendQuery("select", query)
}

func viewLocalDatabase() {
	if db == nil {
		fmt.Println("Local database not set up yet")
		return
	}

	// Show tables in local database
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Printf("\n===== LOCAL DATABASE '%s' =====\n", localDbName)
	fmt.Println("Tables:")

	var table string
	var tableCount int
	for rows.Next() {
		tableCount++
		rows.Scan(&table)
		fmt.Printf("%d. %s\n", tableCount, table)
	}

	if tableCount == 0 {
		fmt.Println("No tables in local database")
		return
	}

	// Ask which table to view
	fmt.Print("\nEnter table number to view data (0 to cancel): ")
	var choice int
	fmt.Scanln(&choice)

	if choice <= 0 || choice > tableCount {
		return
	}

	// Get the selected table name
	rows, _ = db.Query("SHOW TABLES")
	var selectedTable string
	for i := 1; i <= choice; i++ {
		rows.Next()
		rows.Scan(&selectedTable)
	}
	rows.Close()

	// Display records from selected table
	rows, err = db.Query("SELECT * FROM " + selectedTable)
	if err != nil {
		fmt.Printf("Error querying table: %v\n", err)
		return
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		fmt.Printf("Error getting columns: %v\n", err)
		return
	}

	// Print header
	fmt.Printf("\n===== TABLE '%s' =====\n", selectedTable)
	for _, col := range columns {
		fmt.Printf("%s\t", col)
	}
	fmt.Println("\n-----------------------------------------------------------")

	// Prepare for scanning
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Print rows
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			continue
		}

		for _, val := range values {
			switch v := val.(type) {
			case []byte:
				fmt.Printf("%s\t", string(v))
			case nil:
				fmt.Printf("NULL\t")
			default:
				fmt.Printf("%v\t", v)
			}
		}
		fmt.Println()
	}
}

func verifyReplication() {
	if db == nil {
		fmt.Println("Local database not set up yet")
		return
	}

	if !connected {
		fmt.Println("Not connected to master server")
		return
	}

	fmt.Println("\n===== VERIFYING REPLICATION STATUS =====")
	fmt.Println("Requesting verification data from master...")

	// Request table list and row counts from master
	fmt.Fprintf(master, "verify_replication:request\n")

	// The actual verification is handled in listenToMaster when the master responds
}

func readPassword() string {
	fmt.Print("Enter MySQL password: ")

	// In a production environment, you would use a package like "golang.org/x/term"
	// to read passwords securely without displaying them on screen
	// Example:
	// bytePassword, _ := term.ReadPassword(int(syscall.Stdin))
	// return string(bytePassword)

	// For simplicity, we'll just read it directly here
	var password string
	fmt.Scanln(&password)
	return password
}

func main() {
	// Get MySQL credentials for local database
	fmt.Print("Enter MySQL username for local replication: ")
	fmt.Scanln(&dbUser)

	dbPassword = readPassword()

	if dbUser == "" {
		fmt.Println("Warning: Using empty username for database connection")
	}

	var masterAddr string
	fmt.Print("Enter master server address (default: localhost:9999): ")
	fmt.Scanln(&masterAddr)

	if masterAddr == "" {
		masterAddr = "localhost:9999"
	}

	// Try to connect to master
	if !connectToMaster(masterAddr) {
		fmt.Println("Initial connection failed. Will retry in background.")
		// Retry connection in background
		go func() {
			for !connected {
				time.Sleep(5 * time.Second)
				fmt.Println("Attempting to reconnect to master...")
				connectToMaster(masterAddr)
			}
		}()
	}

	// Start the command loop
	for {
		fmt.Println("\n===== SLAVE CLIENT MENU =====")
		fmt.Println("1. Insert Record")
		fmt.Println("2. Update Record")
		fmt.Println("3. Delete Record")
		fmt.Println("4. Query Records")
		fmt.Println("5. View Local Database")
		fmt.Println("6. Verify Replication Status")
		fmt.Println("7. Reconnect to Master")
		fmt.Println("8. Exit Program")

		if !connected {
			fmt.Println("WARNING: Not connected to master server!")
		}

		fmt.Print("Enter choice: ")
		var choice int
		fmt.Scanln(&choice)

		switch choice {
		case 1:
			insertRecord()
		case 2:
			updateRecord()
		case 3:
			deleteRecord()
		case 4:
			selectRecords()
		case 5:
			viewLocalDatabase()
		case 6:
			verifyReplication()
		case 7:
			if connected {
				master.Close()
				connected = false
			}
			connectToMaster(masterAddr)
		case 8:
			fmt.Println("Exiting program...")
			if connected {
				master.Close()
			}
			if db != nil {
				db.Close()
			}
			return
		default:
			fmt.Println("Invalid choice")
		}
	}
}
