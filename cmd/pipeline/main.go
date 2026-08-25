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
	writer := &pipeline.StdoutWriter[int64]{}

	// criterion := func(item pgcr.PostGameCarnageReport) bool {
	// 	return item.ActivityDetails.InstanceId.Int64() != 0
	// }
	//
	// filterFunc := func(ctx context.Context, item pgcr.PostGameCarnageReport) (bool, error) {
	// 	if item.ActivityDetails.InstanceId.Int64() == 0 {
	// 		return false, nil
	// 	}
	// 	return true, nil
	// }

	itemMapper := &pipeline.PgcrMapper{}

	// var filter pipeline.Predicate[pgcr.PostGameCarnageReport]
	// filter = filterFunc

	// This should trigger it?
	err := pipeline.From(reader).
		MapTo(itemMapper).
		WriteTo(ctx, writer)
	if err != nil {
		fmt.Printf("Error running pipeline: %v", err)
	}
}
