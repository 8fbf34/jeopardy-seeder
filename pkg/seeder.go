package pkg

import (
	"database/sql"
	"encoding/json"
	"sync"
	//"syscall"
	"io/ioutil"
	"os"
	//"os/signal"
	"net/http"
	"log"
	"fmt"
	_ "github.com/lib/pq"
)

var connStr string = "postgres://dev:devvydev@localhost/jeopardy"
var urlStr string = "http://jservice.io/api/random"
var DB *sql.DB

type Control struct {
	Nb	int
	Mu	sync.Mutex
	Wg	sync.WaitGroup
}

func Seed() {
	Connect()
	sigc := make(chan os.Signal, 1)
	//signal.Notify(sigc,
	//	syscall.SIGHUP,
	//	syscall.SIGINT,
	//	syscall.SIGTERM,
	//	syscall.SIGQUIT)
	go func() {
		<-sigc
		DB.Close()
		log.Fatal("Received kill signal. Closing databases")
	}()

	// statusChan is used for printing status from all processes
	statusChan := make(chan string)
	defer close(statusChan)

	// setup control structs for requests and database
	//httpControl := Control{}
	//sqlControl := Control{}

	// setup db channel to receieve jeopardy entries for processing and semaphore
	dbChan := make(chan []JeopardyEntry, 10)
	sqlSem := make(chan int, 20)
	defer close(sqlSem)
	defer close(dbChan)
	go func() {
		var insertStr string
		for {
			insertStr = "INSERT INTO entries VALUES "
			select {
			case entries := <-dbChan:
				//tx, err := DB.Begin()
				vals := []interface{}{}
				for _, e := range entries {
					insertStr += "(?, ?, ?, ?),"
					vals = append(vals,
						e.Id,
						e.Question,
						e.Answer,
						e.CategoryId)
				}
				stmt, err := DB.Prepare(insertStr[0:len(insertStr)-2])
				if err != nil {
					statusChan<-fmt.Sprintf("Failed to add entry: %s\nError was: %s",
						entries[0].String(),
						err.Error())
					<-sqlSem
					continue
				}
				defer stmt.Close()

				_, err = stmt.Exec(vals...)
				nbEntries := len(vals)/4
				if err != nil {
					statusChan<-fmt.Sprintf("Failed to add %d entries", nbEntries)
				} else {
					statusChan<-fmt.Sprintf("Successfully added %d entries", nbEntries)
				}
				<-sqlSem
			}
		}
	}()

	go func() {
		for {
			fmt.Println(<-statusChan)
		}
	}()

	// run a ton of parallelized REST requests
	var httpSem = make(chan int, 10)
	for {
		httpSem <- 1 // lock http resources
		go func() {
			statusChan<-"Locked resource for request"
			resp, err := http.Get(urlStr)
			if err != nil {
				statusChan<-err.Error()
				<-httpSem // release resource
				return
			}

			statusChan<-"Received response from request"
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				statusChan<-err.Error()
				<-httpSem // release resource
				return
			}

			var entries []JeopardyEntry
			err = json.Unmarshal(body, &entries)
			if err != nil {
				statusChan<-err.Error()
				<-httpSem // release resource
				return
			}
			statusChan<-fmt.Sprintf("Parsed body to entry: %s", entries[0].String())
			dbChan<-entries
			sqlSem <- 1
			<-httpSem // release resource
		}()
	}


}

func (c *Control) increment() {
	c.Mu.Lock()
	c.Nb = c.Nb + 1
	c.Mu.Unlock()
	c.Wg.Add(1)
}

func (c *Control) decrement() {
	c.Mu.Lock()
	c.Nb = c.Nb - 1
	c.Mu.Unlock()
	c.Wg.Done()
}

func Connect() {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	DB = db
}

