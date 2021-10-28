package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
	_ "modernc.org/sqlite"
)

const driverName = "sqlite"

func main() {
	fn := "db.sqlite"
	db, err := sql.Open(driverName, fn)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`
CREATE TABLE fileHash (
        "hash" TEXT NOT NULL PRIMARY KEY,
        "filename" TEXT,
        "lastChecked" INTEGER
 );`); err != nil {
		log.Fatal(err)
	}

	t0 := time.Now()
	n := 0
	for time.Since(t0) < time.Duration(60)*time.Minute {
		hash := randomString()
		if _, err = lookupHash(fn, hash); err != nil {
			log.Error(err)
			break
		}

		if err = saveHash(fn, hash, hash+".temp"); err != nil {
			log.Error(err)
			break
		}
		n++
	}
	log.Printf("cycles: %v", n)
	row := db.QueryRow("select count(*) from fileHash")
	if err := row.Scan(&n); err != nil {
		log.Fatal(err)
	}

	log.Printf("DB records: %v", n)
}

func randomString() string {
	b := make([]byte, 32)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func saveHash(dbFile string, hash string, fileName string) (err error) {
	// log.WithField("Hash", hash).Println("Saving Hash...")
	db, err := sql.Open(driverName, dbFile)
	if err != nil {
		return fmt.Errorf("could not open database: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.WithField("Error", err).Error("could not close the database")
		}
	}()

	query := `INSERT OR REPLACE INTO fileHash(hash, fileName, lastChecked)
                        VALUES(?, ?, ?);`

	statement, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("could not prepare statement: %v", err)
	}

	defer func() {
		if err := statement.Close(); err != nil {
			log.WithField("Error", err).Error("Could not close the statement")
		}
	}()

	_, err = statement.Exec(
		hash,
		fileName,
		time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("error saving hash to database: %v", err)
	}

	return nil
}

func lookupHash(dbFile string, hash string) (ok bool, err error) {
	// log.WithField("Hash", hash).Println("Looking up hash...")
	db, err := sql.Open(driverName, dbFile)
	if err != nil {
		return false, fmt.Errorf("could not open database: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.WithField("Error", err).Error("could not close the database")
		}
	}()

	query := `SELECT hash, fileName, lastChecked
                                FROM fileHash
                                WHERE hash=?;`

	statement, err := db.Prepare(query)
	if err != nil {
		return false, fmt.Errorf("could not prepare statement: %v", err)
	}

	defer func() {
		if err := statement.Close(); err != nil {
			log.WithField("Error", err).Error("Could not close the statement")
		}
	}()

	rows, err := statement.Query(hash)
	if err != nil {
		return false, fmt.Errorf("error checking database for hash: %v", err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			log.WithField("Error", err).Error("Could not close DB rows")
		}
	}()

	var (
		dbHash      string
		fileName    string
		lastChecked int64
	)
	for rows.Next() {
		err = rows.Scan(&dbHash, &fileName, &lastChecked)
		if err != nil {
			return false, fmt.Errorf("could not read DB row: %v", err)
		}

		if dbHash == hash {
			return true, nil
		}
	}
	return false, nil
}
