package main

import (
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
)

func main() {
	log.Info("Starting...")
	// If the file doesn't exist, create it, or append to the file
	f, err := os.OpenFile("./app.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.WithField("Error", err).Errorln("Could not write to log file.")
	}

	log.SetOutput(io.MultiWriter(os.Stdout, f))

	dbFile := "./db.sqlite"

	_, err = initDatabase(dbFile)
	if err != nil {
		log.WithField("Error", err).Errorln("Failed to open database.")
	}

	for true {
		hash := randomString()

		_, err := lookupHash(dbFile, hash)
		if err != nil {
			log.WithField("Error", err).Errorln("Failed to lookup hash.")
		}

		err = saveHash(dbFile, hash, hash+".temp")
		if err != nil {
			log.WithField("Error", err).Errorln("Failed to save hash to database.")
		}
	}
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(
	rand.NewSource(time.Now().UnixNano()))

func randomString() string {
	b := make([]byte, 32)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func initDatabase(sqlFile string) (*sql.DB, error) {
	isNewDB := false
	if _, err := os.Stat(sqlFile); os.IsNotExist(err) {
		isNewDB = true
	}
	if isNewDB {
		log.WithField("File", sqlFile).Infoln("Creating database")
		file, err := os.Create(sqlFile) // Create SQLite file
		if err != nil {
			return nil, fmt.Errorf("could not create database file: %w", err)
		}
		err = file.Close()
		if err != nil {
			return nil, fmt.Errorf("could not close new database file: %w", err)
		}
		log.Println("Database created")
	}

	db, err := sql.Open("sqlite", sqlFile)
	if err != nil {
		return nil, fmt.Errorf("could not open database: %w", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.WithField("Error", err).Errorln("could not close the database.")
		}
	}(db)

	// To resolve too many files open error
	// If this does not work, then perform a db.close and then open/close for each query.
	// This did not work, application just freezes.
	// db.SetMaxOpenConns(10)
	// db.SetMaxIdleConns(5)
	// db.SetConnMaxIdleTime(30 * time.Second)

	if isNewDB {
		if err := createTable(db); err != nil {
			return nil, fmt.Errorf("could not create the SQL table: %w", err)
		}
	}

	return db, nil
}

func createTable(db *sql.DB) error {
	query := `CREATE TABLE fileHash (
		"hash" TEXT NOT NULL PRIMARY KEY,
		"filename" TEXT,
		"lastChecked" INTEGER
	  );` // SQL Statement for Create Table

	log.WithField("Table", "fileHash").Println("Creating database table.")
	_, err := db.Exec(query)
	// _, err := executeSQL(db, query)
	if err != nil {
		return fmt.Errorf("error creating database table: %w", err)
	}
	log.WithField("Table", "fileHash").Println("Table created")
	return nil
}

func saveHash(dbFile string, hash string, fileName string) error {
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return fmt.Errorf("could not open database: %w", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.WithField("Error", err).Errorln("could not close the database.")
		}
	}(db)

	query := `INSERT OR REPLACE INTO fileHash(hash, fileName, lastChecked)
			VALUES(?, ?, ?);`
	_, err = executeSQL(db, query,
		hash,
		fileName,
		time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("error saving hash to database: %w", err)
	}

	log.WithFields(log.Fields{
		"Hash": hash,
		"File": fileName,
	}).Println("Saved hash to database")
	return nil
}

func lookupHash(dbFile string, hash string) (bool, error) {
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		return false, fmt.Errorf("could not open database: %w", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.WithField("Error", err).Errorln("could not close the database.")
		}
	}(db)

	query := `SELECT hash, fileName, lastChecked
				FROM fileHash
				WHERE hash=?;`
	rows, err := executeSQL(db, query, hash)
	if err != nil {
		return false, fmt.Errorf("error checking database for hash: %w", err)
	}

	defer func(rows *sql.Rows) {
		if rows.Close() != nil {
			log.WithField("Error", err).Errorln("Could not close DB rows")
		}
	}(rows)

	var (
		dbHash      string
		fileName    string
		lastChecked int64
	)
	for rows.Next() {
		err = rows.Scan(&dbHash, &fileName, &lastChecked)
		if err != nil {
			return false, fmt.Errorf("could not read DB row: %w", err)
		}

		log.WithFields(log.Fields{
			"Hash":      dbHash,
			"File":      fileName,
			"TimeStamp": time.Unix(lastChecked, 0).Format("01/02/06 15:04:05"),
		})

		return true, nil
	}

	return false, nil
}

func executeSQL(db *sql.DB, query string, values ...interface{}) (*sql.Rows, error) {
	statement, err := db.Prepare(query) // Prepare SQL Statement
	if err != nil {
		return &sql.Rows{}, fmt.Errorf("could not prepare statement: %w", err)
	}

	result, err := statement.Query(values...) // Execute SQL Statements
	if err != nil {
		return &sql.Rows{}, fmt.Errorf("could not execute query: %w", err)
	}

	return result, nil
}
