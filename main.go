package main

import (
	"pgcr-processing-service/cmd"
)

func main() {
	cmd.NewRootCommand().Execute()
}
