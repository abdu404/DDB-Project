# Project Overview

This project implements a **master-slave database replication system** using Go. The master server maintains the primary database and propagates changes to connected slave servers, keeping them in sync. The system supports **MySQL databases** and provides a **command-line interface** for interacting with both master and slave instances.

## Features

- Master server that accepts connections from slaves  
- Automatic schema and data synchronization for new slaves  
- Real-time replication of `INSERT`, `UPDATE`, and `DELETE` operations  
- Database and table creation/dropping with propagation to slaves  
- Replication verification to check sync status  
- Local database viewing on slave nodes  

---

## 🔧 Prerequisites

- Go 1.24.2 or later  
- MySQL server  
- MySQL driver for Go:  
  ```bash
  go get github.com/go-sql-driver/mysql
  
Installation
Clone the repository:

bash
Copy
Edit
git clone https://github.com/yourusername/database-replication.git
cd database-replication

Install dependencies:

bash
Copy
Edit
go mod download

Configuration
Set up your MySQL server and create a user with appropriate permissions

Note down your MySQL username and password

Decide on a database name to use for replication

Running the System
 Master Server
To run the master server:

bash
Copy
Edit
go run master.go
You will be prompted to:

Enter your database name

Provide MySQL credentials

Interact with the database through a menu system

Slave Client
To run the slave client (in a separate terminal):

bash
Copy
Edit
go run slave.go
You will be prompted to:

Enter MySQL credentials for the local replica database

Connect to the master server (default: localhost:9999)

Interact with the system through the menu

###System Architecture
```bash
+------------------------------+     
|         Master Node          |      
| ---------------------------- |     
|     DB write access          |               
|     Broadcast to slaves      |                               
+------------------------------+      
         ^           ^         
         |           |         
         v            -------------------------------- v       
+-------------------------------+       +----------------------------------+                        
|         Slave Node            |       |         Slave Node               |
| ----------------------------- |       | -------------------------------- |
|       Read-only DB            |       |       Read-only DB              |
|       Listen to MQ            |       |       Listen to MQ              |
+-------------------------------+       +----------------------------------+
