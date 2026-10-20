package writer

import (
	"context"

	"pgcr-processing-service/internal/types/bungie"

	"github.com/deahtstroke/chainmorph"
)

type DeadLetterWriter struct {
	inner chainmorph.ItemWriter[bungie.PostGameCarnageReport]
}

func (w *DeadLetterWriter) Write(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
	if err := w.inner.Write(ctx, pgcr); err != nil {
	}
	return nil
}
