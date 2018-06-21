package pkg

import (
	"database/sql"
	"fmt"
	"log"
)

var connStr string = "postgres://dev:devvydev@localhost/jeopardy?sslmode=disable"
var urlStr string = "http://jservice.io/api/random"
var DB *sql.DB
var dbChan chan []JeopardyEntry
var sqlSem chan int

func SetupSeeder() {
	// setup db channel to receieve jeopardy entries for processing and semaphore
	go func() {
		var insertStr string
		for {
			insertStr = "INSERT INTO entries VALUES "
			select {
			case entries := <-dbChan:
				vals := []interface{}{}
				for _, e := range entries {
					insertStr += "($1, $2, $3, $4),"
					vals = append(vals,
						e.Id,
						e.Question,
						e.Answer,
						e.CategoryId)
				}
				insertStr = insertStr[0:len(insertStr)-1]
				out(insertStr)
				stmt, err := DB.Prepare(insertStr)
				if err != nil {
					out(fmt.Sprintf("Failed to add entry: %s\nError was: %s",
						entries[0].String(),
						err.Error()))
					<-sqlSem
					continue
				}
				defer stmt.Close()

				_, err = stmt.Exec(vals...)
				nbEntries := len(vals)/4
				if err != nil {
					out(fmt.Sprintf("Failed to add %d entries", nbEntries))
				} else {
					out(fmt.Sprintf("Successfully added %d entries", nbEntries))
				}
				<-sqlSem
			}
		}
	}()
}

func ConnectDB() {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	DB = db
}
