package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Database structures
type column struct {
	Name string
	Type int
}

var data_type = [4]string{"INT", "VARCHAR(100)", "FLOAT", "TEXT"}
var tables []string
var currentTable string
var tableAttributes = make(map[string][]column)

// Master-Slave communication
var slaves = make(map[string]net.Conn)
var mu sync.Mutex
var db *sql.DB
var dbName string

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

// Database connection setup
func dbConn(dbn string) {
	cfg := mysql.NewConfig()
	fmt.Print("Enter MySQL username: ")
	fmt.Scanln(&cfg.User)

	cfg.Passwd = readPassword()

	if cfg.User == "" {
		fmt.Println("Warning: Using empty username for database connection")
	}

	var err error
	// First connect without specifying a database
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatalf("Connection error: %v", err)
	}

	// Check if database exists
	err = db.QueryRow("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?", dbn).Scan(&dbn)
	if err != nil {
		if err == sql.ErrNoRows {
			// Database doesn't exist, ask to create it
			fmt.Printf("Database '%s' doesn't exist. Create it? (y/n): ", dbn)
			var create string
			fmt.Scanln(&create)
			if strings.ToLower(create) == "y" {
				_, err = db.Exec("CREATE DATABASE " + dbn)
				if err != nil {
					log.Fatalf("Error creating database: %v", err)
				}
				fmt.Println("Database created successfully.")
			} else {
				log.Fatal("Database doesn't exist and user chose not to create it")
			}
		} else {
			log.Fatalf("Error checking database existence: %v", err)
		}
	}

	// Now connect to the specific database
	cfg.DBName = dbn
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Fatalf("Connection error: %v", err)
	}

	// Verify we can connect to the database
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	fmt.Printf("Successfully connected to database '%s'\n", dbn)
}

// Send database schema to slave for replication
func sendSchemaToSlave(conn net.Conn) {
	// First send the database name
	fmt.Fprintf(conn, "init_replication:%s\n", dbName)

	// Send CREATE DATABASE statement
	fmt.Fprintf(conn, "create_db:%s\n", dbName)

	// For each table, send its schema
	for _, tableName := range tables {
		// Get CREATE TABLE statement
		var tableDefinition string
		err := db.QueryRow("SHOW CREATE TABLE "+tableName).Scan(&tableName, &tableDefinition)
		if err != nil {
			fmt.Printf("Error getting CREATE TABLE for %s: %v\n", tableName, err)
			continue
		}

		// Log the full CREATE TABLE statement for debugging
		fmt.Printf("Sending CREATE TABLE statement to slave: %s\n", tableDefinition)

		// Send the CREATE TABLE statement to the slave
		// Make sure to encode any newlines or special characters
		encodedDef := strings.ReplaceAll(tableDefinition, "\n", " ")
		fmt.Fprintf(conn, "create_table:%s\n", encodedDef)

		// Now dump all data from this table
		// First check if the table has data
		var rowCount int
		err = db.QueryRow("SELECT COUNT(*) FROM " + tableName).Scan(&rowCount)
		if err != nil {
			fmt.Printf("Error counting rows in %s: %v\n", tableName, err)
			continue
		}

		if rowCount == 0 {
			fmt.Printf("Table %s is empty, skipping data sync\n", tableName)
			continue
		}

		fmt.Printf("Syncing %d rows from table %s\n", rowCount, tableName)

		// Use batched processing for large tables
		const batchSize = 100
		for offset := 0; offset < rowCount; offset += batchSize {
			rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d",
				tableName, batchSize, offset))
			if err != nil {
				fmt.Printf("Error selecting data from %s: %v\n", tableName, err)
				continue
			}

			columns, err := rows.Columns()
			if err != nil {
				rows.Close()
				fmt.Printf("Error getting columns for %s: %v\n", tableName, err)
				continue
			}

			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			// For each row in the batch
			rowNum := 0
			for rows.Next() {
				rowNum++
				err = rows.Scan(scanArgs...)
				if err != nil {
					fmt.Printf("Error scanning row: %v\n", err)
					continue
				}

				// Construct INSERT statement
				insertQuery := fmt.Sprintf("INSERT INTO %s (", tableName)
				valueStrings := make([]string, len(columns))

				// Add column names
				for i, colName := range columns {
					if i > 0 {
						insertQuery += ", "
					}
					insertQuery += colName
				}
				insertQuery += ") VALUES ("

				// Convert scan values to strings
				for i, val := range values {
					var strVal string
					if val == nil {
						strVal = "NULL"
					} else {
						switch v := val.(type) {
						case []byte:
							strVal = "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
						case string:
							strVal = "'" + strings.ReplaceAll(v, "'", "''") + "'"
						default:
							strVal = fmt.Sprintf("%v", v)
						}
					}
					valueStrings[i] = strVal
				}

				// Add value strings
				insertQuery += strings.Join(valueStrings, ", ") + ")"

				// Send the INSERT statement to the slave
				fmt.Fprintf(conn, "sync_data:%s\n", insertQuery)
			}
			rows.Close()

			fmt.Printf("Sent batch of %d rows from table %s (offset %d)\n",
				rowNum, tableName, offset)
		}
	}

	// Signal end of schema replication
	fmt.Fprintf(conn, "replication_complete:done\n")
	fmt.Printf("Schema and data sent to slave: %s\n", conn.RemoteAddr().String())
}

// Slave connection handler
func handleSlaveConnection(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	mu.Lock()
	slaves[addr] = conn
	mu.Unlock()
	fmt.Println("Slave connected:", addr)

	// Send schema to new slave for replication
	sendSchemaToSlave(conn)

	defer func() {
		mu.Lock()
		delete(slaves, addr)
		mu.Unlock()
		conn.Close()
		fmt.Println("Slave disconnected:", addr)
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		request := scanner.Text()
		parts := strings.SplitN(request, ":", 2)
		if len(parts) != 2 {
			fmt.Fprintf(conn, "error:invalid request format\n")
			continue
		}

		operation := parts[0]
		query := parts[1]

		// Handle operations
		switch operation {
		case "insert":
			executeQuery(query, conn)
		case "update":
			executeQuery(query, conn)
		case "delete":
			executeQuery(query, conn)
		case "select":
			executeSelect(query, conn)
		case "verify_replication":
			handleVerifyReplication(conn)
		case "get_table_schema":
			sendTableSchema(query, conn)
		default:
			fmt.Fprintf(conn, "error:unsupported operation\n")
		}
	}
}

// Handle replication verification requests
func handleVerifyReplication(conn net.Conn) {
	fmt.Println("Received replication verification request from:", conn.RemoteAddr())

	// Get table information
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		fmt.Fprintf(conn, "error:Failed to get tables: %v\n", err)
		return
	}
	defer rows.Close()

	// Start verification response
	fmt.Fprintf(conn, "verification_data:begin\n")

	// Send info for each table
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

		// Send table info
		fmt.Fprintf(conn, "table:%s:%d\n", tableName, rowCount)
	}

	// End verification response
	fmt.Fprintf(conn, "verification_data:end\n")
}

// Execute query and return result to slave
func executeQuery(query string, conn net.Conn) {
	_, err := db.Exec(query)
	if err != nil {
		fmt.Fprintf(conn, "error:%v\n", err)
		return
	}
	fmt.Fprintf(conn, "success:query executed\n")
	fmt.Println("Query Executed Succesfuly")

	// Propagate the change to all slaves except the one that sent the query
	mu.Lock()
	for _, slaveConn := range slaves {
		if slaveConn != conn { // Skip the slave that sent the query
			fmt.Fprintf(slaveConn, "replicate_query:%s\n", query)
		}
	}
	mu.Unlock()
}

// Execute SELECT query and return results to slave
func executeSelect(query string, conn net.Conn) {
	rows, err := db.Query(query)
	if err != nil {
		fmt.Fprintf(conn, "error:%v\n", err)
		return
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(conn, "error:%v\n", err)
		return
	}

	// Prepare result holders
	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Start with success header
	fmt.Fprintf(conn, "success:%d\n", len(columns))

	// Send column names
	colNames := strings.Join(columns, ",")
	fmt.Fprintf(conn, "%s\n", colNames)

	// Send data rows
	rowCount := 0
	for rows.Next() {
		rowCount++
		err = rows.Scan(scanArgs...)
		if err != nil {
			continue
		}

		var rowData []string
		for _, v := range values {
			var strValue string
			if v == nil {
				strValue = "NULL"
			} else {
				switch v := v.(type) {
				case []byte:
					strValue = string(v)
				default:
					strValue = fmt.Sprintf("%v", v)
				}
			}
			rowData = append(rowData, strValue)
		}
		fmt.Fprintf(conn, "%s\n", strings.Join(rowData, ","))
	}

	// End marker
	fmt.Fprintf(conn, "END\n")
}

// Load existing tables from database
func loadExistingTables() {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Fatalf("Error loading tables: %v", err)
	}
	defer rows.Close()

	tables = tables[:0]
	var table string
	for rows.Next() {
		rows.Scan(&table)
		tables = append(tables, table)
		// Load attributes for each table
		GetColumnInfo(table)
	}
}

func GetColumnInfo(table string) {
	attrs := []column{}
	rows, err := db.Query("DESCRIBE " + table)
	if err != nil {
		log.Fatalf("Describe error: %v", err)
	}
	defer rows.Close()

	var field, colType, nul, key, defVal, extra string
	for rows.Next() {
		rows.Scan(&field, &colType, &nul, &key, &defVal, &extra)
		if field == "id" {
			continue
		}
		idx := 0
		for i, t := range data_type {
			if strings.Contains(colType, strings.ToLower(t)) {
				idx = i
				break
			}
		}
		attrs = append(attrs, column{Name: field, Type: idx})
	}
	tableAttributes[table] = attrs
}

func TableExists(tableName string) bool {
	query := fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName)
	row := db.QueryRow(query)
	var existingTable string
	err := row.Scan(&existingTable)
	return err == nil
}

func CreateTable(name string) {
	var num int
	fmt.Print("\nEnter number of attributes: ")
	fmt.Scanln(&num)

	attrs := make([]column, num)

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY AUTO_INCREMENT", name)
	for i := 0; i < num; i++ {
		fmt.Printf("\nEnter name for column %d: ", i+1)
		fmt.Scanln(&attrs[i].Name)

		fmt.Println("Choose data type:")
		for j, dt := range data_type {
			fmt.Printf("%d: %s\n", (j + 1), dt)
		}
		fmt.Print("Enter choice: ")
		var x int
		fmt.Scanln(&x)
		attrs[i].Type = x - 1

		query += fmt.Sprintf(", %s %s", attrs[i].Name, data_type[attrs[i].Type])
	}
	query += ")"

	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	fmt.Println("Table created successfully.")
	tableAttributes[name] = attrs

	// Notify slaves about the new table
	notifySlaves("Table created: " + name)

	// Execute table creation manually before sending to slaves
	// This ensures we can generate a proper SHOW CREATE TABLE statement
	time.Sleep(500 * time.Millisecond) // Short delay to make sure table is created

	// Get the full CREATE TABLE statement to send to slaves
	var tableDefinition string
	err = db.QueryRow("SHOW CREATE TABLE "+name).Scan(&name, &tableDefinition)
	if err != nil {
		fmt.Printf("Error getting CREATE TABLE statement: %v\n", err)
		// Fall back to our original query if we can't get the full definition
		tableDefinition = query
	}

	// Log the statement for debugging
	fmt.Printf("CREATE TABLE statement to send to slaves: %s\n", tableDefinition)

	// Remove any newlines and special characters that might interfere with transmission
	encodedDef := strings.ReplaceAll(tableDefinition, "\n", " ")
	encodedDef = strings.ReplaceAll(encodedDef, "\r", " ")

	// Send create table query to all slaves for replication
	mu.Lock()
	for _, conn := range slaves {
		fmt.Fprintf(conn, "create_table:%s\n", encodedDef)
	}
	mu.Unlock()
}

func notifySlaves(message string) {
	mu.Lock()
	defer mu.Unlock()
	for addr, conn := range slaves {
		_, err := fmt.Fprintf(conn, "notification:%s\n", message)
		if err != nil {
			fmt.Printf("Failed to notify slave %s: %v\n", addr, err)
		}
	}
}

func DropTable() {
	fmt.Printf("Are you sure you want to drop table '%s'? (y/n): ", currentTable)
	var confirm string
	fmt.Scanln(&confirm)

	if strings.ToLower(confirm) == "y" {
		dropQuery := "DROP TABLE " + currentTable
		_, err := db.Exec(dropQuery)
		if err != nil {
			fmt.Printf("Error dropping table: %v\n", err)
		} else {
			fmt.Println("Table dropped successfully.")
			// Remove from tables list
			for i, table := range tables {
				if table == currentTable {
					tables = append(tables[:i], tables[i+1:]...)
					break
				}
			}
			delete(tableAttributes, currentTable)

			// Notify slaves about the dropped table
			notifySlaves("Table dropped: " + currentTable)

			// Send drop table query to all slaves for replication
			mu.Lock()
			for _, conn := range slaves {
				fmt.Fprintf(conn, "replicate_query:%s\n", dropQuery)
			}
			mu.Unlock()
		}
	} else {
		fmt.Println("Table drop cancelled.")
	}
}

func DropDatabase() {
	fmt.Printf("Are you sure you want to drop database '%s'? (y/n): ", dbName)
	var confirm string
	fmt.Scanln(&confirm)

	if strings.ToLower(confirm) == "y" {
		dropQuery := "DROP DATABASE " + dbName
		_, err := db.Exec(dropQuery)
		if err != nil {
			fmt.Printf("Error dropping database: %v\n", err)
		} else {
			fmt.Println("Database dropped successfully.")

			// Notify slaves to drop their copies of the database
			mu.Lock()
			for _, conn := range slaves {
				fmt.Fprintf(conn, "drop_database:%s\n", dbName)
			}
			mu.Unlock()

			// Close all slave connections
			mu.Lock()
			for addr, conn := range slaves {
				conn.Close()
				fmt.Printf("Closed connection to slave: %s\n", addr)
			}
			slaves = make(map[string]net.Conn)
			mu.Unlock()

			os.Exit(0)
		}
	} else {
		fmt.Println("Database drop cancelled.")
	}
}

func InsertRecord() {
	attrs := tableAttributes[currentTable]
	query := fmt.Sprintf("INSERT INTO %s (", currentTable)
	values := make([]interface{}, len(attrs))

	for i, attr := range attrs {
		query += attr.Name
		if i != len(attrs)-1 {
			query += ", "
		} else {
			query += ") VALUES ("
		}
	}

	for i := range attrs {
		query += "?"
		if i != len(attrs)-1 {
			query += ", "
		} else {
			query += ")"
		}
	}

	for i, attr := range attrs {
		fmt.Printf("Enter value for %s: ", attr.Name)
		switch data_type[attr.Type] {
		case "INT":
			var v int
			fmt.Scanln(&v)
			values[i] = v
		case "FLOAT":
			var v float64
			fmt.Scanln(&v)
			values[i] = v
		default:
			var v string
			fmt.Scanln(&v)
			values[i] = v
		}
	}

	_, err := db.Exec(query, values...)
	if err != nil {
		fmt.Printf("Insert error: %v\n", err)
	} else {
		fmt.Println("Record inserted successfully.")

		// Prepare query with actual values for slaves
		replicaQuery := fmt.Sprintf("INSERT INTO %s (", currentTable)
		valuesList := make([]string, len(attrs))

		for i, attr := range attrs {
			replicaQuery += attr.Name
			if i != len(attrs)-1 {
				replicaQuery += ", "
			} else {
				replicaQuery += ") VALUES ("
			}

			// Format the value
			var strVal string
			if values[i] == nil {
				strVal = "NULL"
			} else {
				switch v := values[i].(type) {
				case string:
					strVal = "'" + strings.ReplaceAll(v, "'", "''") + "'"
				default:
					strVal = fmt.Sprintf("%v", v)
				}
			}
			valuesList[i] = strVal
		}

		replicaQuery += strings.Join(valuesList, ", ") + ")"

		// Send insert query to all slaves for replication
		mu.Lock()
		for _, conn := range slaves {
			fmt.Fprintf(conn, "replicate_query:%s\n", replicaQuery)
		}
		mu.Unlock()
	}
}

func UpdateRecord() {
	attrs := tableAttributes[currentTable]
	fmt.Print("Enter ID to update: ")
	var id int
	fmt.Scanln(&id)

	// First verify the record exists
	row := db.QueryRow(fmt.Sprintf("SELECT id FROM %s WHERE id = ?", currentTable), id)
	var foundID int
	if err := row.Scan(&foundID); err != nil {
		fmt.Printf("Record with ID %d not found\n", id)
		return
	}

	setClause := ""
	values := []interface{}{}
	updateFields := []string{} // Track which fields are being updated

	for _, attr := range attrs {
		fmt.Printf("Enter new value for %s (leave empty to keep current): ", attr.Name)
		var input string
		fmt.Scanln(&input)

		if input == "" {
			continue // skip updating this field
		}

		if setClause != "" {
			setClause += ", "
		}

		setClause += fmt.Sprintf("%s = ?", attr.Name)
		updateFields = append(updateFields, attr.Name)

		switch data_type[attr.Type] {
		case "INT":
			var v int
			fmt.Sscanf(input, "%d", &v)
			values = append(values, v)
		case "FLOAT":
			var v float64
			fmt.Sscanf(input, "%f", &v)
			values = append(values, v)
		default:
			values = append(values, input)
		}
	}

	if setClause == "" {
		fmt.Println("No fields to update.")
		return
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", currentTable, setClause)
	values = append(values, id)

	_, err := db.Exec(query, values...)
	if err != nil {
		fmt.Printf("Update error: %v\n", err)
	} else {
		fmt.Println("Record updated successfully.")

		// Prepare the replica query with actual values
		replicaSetClause := ""

		for i, fieldName := range updateFields {
			if i > 0 {
				replicaSetClause += ", "
			}

			// Format the value
			var strVal string
			v := values[i]
			if v == nil {
				strVal = "NULL"
			} else {
				switch v := v.(type) {
				case string:
					strVal = "'" + strings.ReplaceAll(v, "'", "''") + "'"
				default:
					strVal = fmt.Sprintf("%v", v)
				}
			}

			replicaSetClause += fmt.Sprintf("%s = %s", fieldName, strVal)
		}

		replicaQuery := fmt.Sprintf("UPDATE %s SET %s WHERE id = %d", currentTable, replicaSetClause, id)

		// Send update query to all slaves for replication
		mu.Lock()
		for _, conn := range slaves {
			fmt.Fprintf(conn, "replicate_query:%s\n", replicaQuery)
		}
		mu.Unlock()
	}
}

func DeleteRecord() {
	fmt.Print("Enter ID to delete: ")
	var id int
	fmt.Scanln(&id)

	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", currentTable)
	_, err := db.Exec(query, id)
	if err != nil {
		fmt.Printf("Delete error: %v\n", err)
	} else {
		fmt.Println("Record deleted successfully.")

		// Send delete query to all slaves for replication
		replicaQuery := fmt.Sprintf("DELETE FROM %s WHERE id = %d", currentTable, id)

		mu.Lock()
		for _, conn := range slaves {
			fmt.Fprintf(conn, "replicate_query:%s\n", replicaQuery)
		}
		mu.Unlock()
	}
}

func DisplayRecords() {
	attrs := tableAttributes[currentTable]
	rows, err := db.Query("SELECT * FROM " + currentTable)
	if err != nil {
		fmt.Printf("Error retrieving records: %v\n", err)
		return
	}
	defer rows.Close()

	fmt.Print("id\t")
	for _, attr := range attrs {
		fmt.Printf("%s\t", attr.Name)
	}
	fmt.Println("\n-----------------------------------------------------------")

	cols := make([]interface{}, len(attrs)+1)
	colPtrs := make([]interface{}, len(attrs)+1)

	for i := range cols {
		colPtrs[i] = &cols[i]
	}

	for rows.Next() {
		err := rows.Scan(colPtrs...)
		if err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			continue
		}

		for _, col := range cols {
			switch val := col.(type) {
			case []byte:
				fmt.Printf("%s\t", string(val))
			default:
				fmt.Printf("%v\t", val)
			}
		}
		fmt.Println()
	}
}

func createNewTable() {
	fmt.Print("\nEnter new table name: ")
	var tableName string
	fmt.Scanln(&tableName)
	if tableName == "" {
		fmt.Println("Table name cannot be empty")
		return
	}

	if TableExists(tableName) {
		fmt.Println("Table already exists.")
		return
	}

	CreateTable(tableName)
	tables = append(tables, tableName)
	currentTable = tableName
	tableMenu()
}

func selectTable() {
	fmt.Println("\nAvailable Tables:")
	for i, table := range tables {
		fmt.Printf("%d. %s\n", i+1, table)
	}
	fmt.Print("Select table (number): ")

	var choice int
	fmt.Scanln(&choice)

	if choice < 1 || choice > len(tables) {
		fmt.Println("Invalid table selection")
		return
	}

	currentTable = tables[choice-1]
	tableMenu()
}

func tableMenu() {
	for {
		fmt.Printf("\n===== TABLE '%s' MENU =====\n", currentTable)
		fmt.Println("1. Insert Record")
		fmt.Println("2. Update Record")
		fmt.Println("3. Delete Record")
		fmt.Println("4. Display Records")
		fmt.Println("5. Drop Table")
		fmt.Println("6. Back to Main Menu")
		fmt.Print("Enter choice: ")

		var choice int
		fmt.Scanln(&choice)

		switch choice {
		case 1:
			InsertRecord()
		case 2:
			UpdateRecord()
		case 3:
			DeleteRecord()
		case 4:
			DisplayRecords()
		case 5:
			DropTable()
			return
		case 6:
			return
		default:
			fmt.Println("Invalid choice")
		}
	}
}

func startServer() {
	ln, err := net.Listen("tcp", ":9999")
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	fmt.Println("Master server started on port 9999")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleSlaveConnection(conn)
	}
}

func main() {
	fmt.Print("\nEnter your database name: ")
	fmt.Scanln(&dbName)
	if dbName == "" {
		log.Fatal("Database name cannot be empty")
	}
	dbConn(dbName)

	// Load existing tables
	loadExistingTables()

	// Start server in a goroutine
	go startServer()

mainMenu:
	for {
		fmt.Println("\n===== MAIN MENU =====")
		fmt.Println("1. Create New Table")
		fmt.Println("2. Select Existing Table")
		fmt.Println("3. List Connected Slaves")
		fmt.Println("4. Drop Database")
		fmt.Println("5. Exit Program")
		fmt.Print("Enter choice: ")

		var choice int
		fmt.Scanln(&choice)

		switch choice {
		case 1:
			createNewTable()
		case 2:
			if len(tables) == 0 {
				fmt.Println("No tables exist yet. Please create a table first.")
				continue
			}
			selectTable()
		case 3:
			mu.Lock()
			fmt.Println("Connected slaves:")
			if len(slaves) == 0 {
				fmt.Println("No slaves connected")
			} else {
				for addr := range slaves {
					fmt.Println("-", addr)
				}
			}
			mu.Unlock()
		case 4:
			DropDatabase()
		case 5:
			fmt.Println("Exiting program...")
			break mainMenu
		default:
			fmt.Println("Invalid choice")
		}
	}
}

// Send a specific table's schema to a slave
func sendTableSchema(tableName string, conn net.Conn) {
	fmt.Printf("Slave requested schema for table '%s'\n", tableName)

	// Check if table exists
	if !TableExists(tableName) {
		fmt.Fprintf(conn, "error:table '%s' does not exist on master\n", tableName)
		return
	}

	// Get CREATE TABLE statement
	var tableDefinition string
	err := db.QueryRow("SHOW CREATE TABLE "+tableName).Scan(&tableName, &tableDefinition)
	if err != nil {
		fmt.Printf("Error getting CREATE TABLE for %s: %v\n", tableName, err)
		fmt.Fprintf(conn, "error:Failed to get table schema: %v\n", err)
		return
	}

	// Log the full CREATE TABLE statement for debugging
	fmt.Printf("Sending CREATE TABLE statement to slave: %s\n", tableDefinition)

	// Send the CREATE TABLE statement to the slave - ensure any newlines are encoded
	encodedDef := strings.ReplaceAll(tableDefinition, "\n", " ")
	fmt.Fprintf(conn, "create_table:%s\n", encodedDef)
	fmt.Printf("Sent schema for table '%s' to slave\n", tableName)

	// Now send all data for this table
	sendTableData(tableName, conn)
}

// Send all data from a table to a slave
func sendTableData(tableName string, conn net.Conn) {
	// First check if the table has data
	var rowCount int
	err := db.QueryRow("SELECT COUNT(*) FROM " + tableName).Scan(&rowCount)
	if err != nil {
		fmt.Printf("Error counting rows in %s: %v\n", tableName, err)
		return
	}

	if rowCount == 0 {
		fmt.Printf("Table %s is empty, skipping data sync\n", tableName)
		return
	}

	fmt.Printf("Syncing %d rows from table %s\n", rowCount, tableName)

	// Use batched processing for large tables
	const batchSize = 100
	for offset := 0; offset < rowCount; offset += batchSize {
		rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d",
			tableName, batchSize, offset))
		if err != nil {
			fmt.Printf("Error selecting data from %s: %v\n", tableName, err)
			continue
		}

		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			fmt.Printf("Error getting columns for %s: %v\n", tableName, err)
			continue
		}

		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// For each row in the batch
		rowNum := 0
		for rows.Next() {
			rowNum++
			err = rows.Scan(scanArgs...)
			if err != nil {
				fmt.Printf("Error scanning row: %v\n", err)
				continue
			}

			// Construct INSERT statement
			insertQuery := fmt.Sprintf("INSERT INTO %s (", tableName)
			valueStrings := make([]string, len(columns))

			// Add column names
			for i, colName := range columns {
				if i > 0 {
					insertQuery += ", "
				}
				insertQuery += colName
			}
			insertQuery += ") VALUES ("

			// Convert scan values to strings
			for i, val := range values {
				var strVal string
				if val == nil {
					strVal = "NULL"
				} else {
					switch v := val.(type) {
					case []byte:
						strVal = "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
					case string:
						strVal = "'" + strings.ReplaceAll(v, "'", "''") + "'"
					default:
						strVal = fmt.Sprintf("%v", v)
					}
				}
				valueStrings[i] = strVal
			}

			// Add value strings
			insertQuery += strings.Join(valueStrings, ", ") + ")"

			// Send the INSERT statement to the slave
			fmt.Fprintf(conn, "sync_data:%s\n", insertQuery)
		}
		rows.Close()

		fmt.Printf("Sent batch of %d rows from table %s (offset %d)\n",
			rowNum, tableName, offset)
	}
}
