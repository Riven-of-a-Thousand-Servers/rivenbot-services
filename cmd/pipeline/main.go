package main

import (
	"context"
	"fmt"

	"pgcr-processing-service/internal/pipeline"
	"pgcr-processing-service/internal/types/pgcr"
)

func main() {
	ctx := context.Background()
	reader := &pipeline.FileReader[pgcr.PostGameCarnageReport]{
		Path: "example.json",
	}
	writer := &pipeline.StdoutWriter[pgcr.PostGameCarnageReport]{}

	// This should trigger it?
	if err := pipeline.From(reader).WriteTo(ctx, writer); err != nil {
		fmt.Printf("Error running pipeline: %v", err)
	}
}
