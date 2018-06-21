# jeopardy-seeder
A scraping application that seeds my local database. No, devvydev is not production.

This application is highly parallelized and utilizes semaphores to maintain a certain number of concurrent processes in regards to both HTTP request scrapers and SQL seeder functions. 

The executeable can be compiled with 

```bash
go build cmd/jeopardy-scraper.go
```

or download binary releases from GitHub for your platform. 
