package pkg

import "fmt"

var statusChan chan string

func out(s string) {
	statusChan<-s
}

func StatusMonitor() {
	go func() {
		for {
			fmt.Println(<-statusChan)
		}
	}()
}
