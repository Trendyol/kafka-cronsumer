package main

import (
	"kafka-exception-iterator/config"
	"os"
)

func main() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "dev"
	}

	applicationConfig, err := config.New("./example", "config", env)

	if err != nil {
		panic("application config read failed: " + err.Error())
	}
	applicationConfig.Print()

	select {}
}
