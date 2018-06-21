package pkg

import (
	"fmt"
	"encoding/json"
	"net/http"
	"io/ioutil"
)

var httpSem chan int

func Scrape() {
	// run a ton of parallelized REST requests
	for {
		httpSem <- 1 // lock http resources
		go func() {
			out("Locked resource for request")
			resp, err := http.Get(config.RestEndpoint)
			if err != nil {
				out(err.Error())
				<-httpSem // release resource
				return
			}

			out("Received response from request")
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				out(err.Error())
				<-httpSem // release resource
				return
			}

			var entries []JeopardyEntry
			err = json.Unmarshal(body, &entries)
			if err != nil {
				out(err.Error())
				<-httpSem // release resource
				return
			}
			out(fmt.Sprintf("Parsed body to entry: %s", entries[0].String()))
			dbChan<-entries
			sqlSem <- 1
			<-httpSem // release resource
		}()
	}
}
