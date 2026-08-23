package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type ItemReader[T any] interface {
	ReadFrom(context.Context) (T, error)
}

type ItemWriter[T any] interface {
	WriteTo(context.Context, T) error
}

type Pipeline[T any] struct {
	pull func(context.Context) (T, bool, error)
}

type FileReader[T any] struct {
	Path string
}

func (f *FileReader[T]) ReadFrom(ctx context.Context) (T, error) {
	var zero T
	content, err := os.ReadFile(f.Path)
	if err != nil {
		return zero, err
	}

	var b T
	if err := json.Unmarshal(content, &b); err != nil {
		return zero, err
	}

	return b, nil
}

type StdoutWriter[T any] struct{}

func (o *StdoutWriter[T]) WriteTo(ctx context.Context, item T) error {
	fmt.Println(item)
	return nil
}

func From[T any](itemReader ItemReader[T]) *Pipeline[T] {
	return &Pipeline[T]{
		pull: func(ctx context.Context) (T, bool, error) {
			result, err := itemReader.ReadFrom(ctx)
			if err != nil {
				var zero T
				return zero, false, err
			}

			return result, true, nil
		},
	}
}

func (p *Pipeline[T]) WriteTo(ctx context.Context, itemWriter ItemWriter[T]) error {
	res, ok, err := p.pull(ctx)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("Upstream operations not ok")
	}

	return itemWriter.WriteTo(ctx, res)
}
