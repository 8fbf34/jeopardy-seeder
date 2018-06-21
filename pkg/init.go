package pkg

func init() {
	statusChan = make(chan string)
	httpSem = make(chan int, 10)
	dbChan = make(chan []JeopardyEntry, 10)
	sqlSem = make(chan int, 20)
}
