package pkg

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

var sigc chan os.Signal

func SignalMonitor() {
	go func() {
		defer close(sqlSem)
		defer close(dbChan)
		defer close(statusChan)
		defer close(httpSem)
		sigc = make(chan os.Signal, 1)
		signal.Notify(sigc,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGQUIT)
		<-sigc
		DB.Close()
		log.Fatal("Received kill signal.\nStopping program.\nDisconnecting databases.\nKilling sessions.")
	}()
}

func InputMonitor() {
	// TODO: Monitor user input for 'q' to quit application
	return
}

