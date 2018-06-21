package pkg

import (
	"fmt"
	"encoding/json"
	"io/ioutil"
)

var defaultConfigFile string = "config.json"
var defaultDbName string = "jeopardy"
var defaultHostname string = "localhost"
var defaultPort int = 5432
var defaultUsername string = "dev"
var defaultPassword string = "devvydev"
var defaultSslMode string = "disable"
var defaultEndpoint string = "http://jservice.io/api/random"
var config Config

func newConfig() Config {
	defaultConfig := Config{defaultDbName,
				defaultHostname,
				defaultPort,
				defaultUsername,
				defaultPassword,
				defaultSslMode,
				defaultEndpoint}

	configFile, err := ioutil.ReadFile(defaultConfigFile)
	if err != nil {
		return defaultConfig
	}

	var c Config

	err = json.Unmarshal(configFile, &c)
	if err != nil {
		return defaultConfig
	}

	return c
}

func (c Config) DbUri() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s",
		c.Username,
		c.Password,
		c.Hostname,
		c.DbName,
		c.SslMode)
}

