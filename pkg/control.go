package pkg

import (
	_ "github.com/lib/pq"
)

func Start() {
	SignalMonitor()
	InputMonitor()
	StatusMonitor()
	ConnectDB()
	SetupSeeder()
	Scrape()
}

