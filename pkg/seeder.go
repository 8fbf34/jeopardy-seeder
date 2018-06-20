package pkg

import (
	"database/sql"
	"encoding/json"
	"sync"
	"syscall"
	"os"
	"net/http"
	_ "github.com/lib/pq"
)

var connStr string = "postgres://dev:devvydev@localhost/jeopardy"
var urlStr string = "http://jservice.io/api/random"
var DB sql.DB = nil

type Control struct {
	Nb	int
	Mu	sync.Mutex
	Wg	sync.WaitGroup
}

func Seed() {
	Connect()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		<-sigc
		DB.Close()
		log.Fatal("Received kill signal. Closing databases")
	}

	statusChan := make(chan string)

	// setup control structs for requests and database
	httpControl := Control{}
	sqlControl := Control{}

	// setup db channel to receieve jeopardy entries for processing
	dbChan := make(chan JeopardyEntry, 10)
	go func() {
		tx, err := DB.Begin()
		if err != nil {
			statusChan<-"Failed to initiate database transaction"
		} else {
			defer DB.Commit()
			for {
				select {
				case entry := <-dbChan
					sqlControl.increment()
					defer sqlControl.decrement()
					_, err = tx.Exec(`INSERT INTO entries VALUES (?, ?, ?, ?)`,
						entry.Id,
						entry.Question,
						entry.Answer,
						entry.CategoryId)
					if err != nil {
						statusChan<-fmt.Sprintf("Failed to add entry: %s", entry.String())
					} else {
						statusChan<-fmt.Sprintf("Successfully added:  %s", entry.String())
					}
				}
			}
		}
	}

	// run a ton of parallelized REST requests
	var sem = make(chan int, 10)
	for {
		go func() {
			sem <- 1 // lock resource
			resp, err := http.Get(urlStr)
			if err != nil {
				return
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return
			}
			var entry JeopardyEntry
			err = json.Unmarshal(body, &entry)
			if err != nil {
				return
			}
			<-sem // release resource
			dbChan<-entry
		}
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
	DB = &db
}

