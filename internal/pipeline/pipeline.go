package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"pgcr-processing-service/internal/types/pgcr"
)

// Pipeline represents a series of sequentially chaining steps that are taken
// to process an item. They are lazily evaluated until a terminator operation
// is invoked like WriteTo
type Pipeline[T any] struct {
	pull func(context.Context) (T, bool, error)
}

// An ItemReader represents any step that is able to read an arbitrary item of type T
// and chain it downstream to the proceeding operations
type ItemReader[T any] interface {
	ReadFrom(context.Context) (T, error)
}

// An ItemWriter represents the termination step that usually involves writing
// the transformed data to a data sink, whether it'd be a database, a file, or Stdout,
// this step triggers the entire pipeline of steps
type ItemWriter[T any] interface {
	WriteTo(context.Context, T) error
}

// An ItemFilter will filter out the upstream item(s) based on some criterion
type ItemFilter[T any] interface {
	Accept(context.Context, T) (bool, error)
}

type ItemMapper[T any, R any] interface {
	Map(context.Context, T) (R, error)
}

// Custom made reader that reads from a file with location Path
type FileReader[T any] struct {
	// The path of the file to read
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

// StdoutWriter is a simple no-op writer that prints the result(s) to the console
type StdoutWriter[T any] struct{}

func (o *StdoutWriter[T]) WriteTo(ctx context.Context, item T) error {
	_, err := fmt.Printf("<Item>: %v", item)
	return err
}

// The Predicate type wraps a function that evaluates an item of type T
// and returns true/false or a fatal error when something occurs
type Predicate[T any] func(context.Context, T) (bool, error)

func (p Predicate[T]) Accept(ctx context.Context, item T) (bool, error) {
	return p(ctx, item)
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

func (p *Pipeline[T]) Filter(itemFilter ItemFilter[T]) *Pipeline[T] {
	// The reason why we take the previous pull value from the pipeline
	// is so we can fulfill the closure
	prev := p.pull
	return &Pipeline[T]{
		pull: func(ctx context.Context) (T, bool, error) {
			var zero T
			item, ok, err := prev(ctx)
			if !ok || err != nil {
				return zero, false, err
			}

			ok, err = itemFilter.Accept(ctx, item)
			if !ok || err != nil {
				return zero, false, err
			}

			return item, true, nil
		},
	}
}

// This step lets you define a custom function that satisfies the
// type definition of a Preidcate, wraps it inside a Predicate, and
// calls Filter itself
func (p *Pipeline[T]) FilterFunc(f func(context.Context, T) (bool, error)) *Pipeline[T] {
	return p.Filter(Predicate[T](f))
}

// If is an even-simpler function that does not care about context or
// errors, it receives a function that returns true/false and filters based
// on that criteria
func (p *Pipeline[T]) If(f func(item T) bool) *Pipeline[T] {
	return p.Filter(Predicate[T](func(ctx context.Context, t T) (bool, error) {
		return f(t), nil
	}))
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

type PgcrMapper struct {}

func (m *PgcrMapper) Map(ctx context.Context, item pgcr.PostGameCarnageReport) (int64, error) {
	return item.ActivityDetails.InstanceId.Int64(), nil
}

func (p *Pipeline[T]) MapTo[R any](itemMapper ItemMapper[T, R]) *Pipeline[R] {
	prev := p.pull
	return &Pipeline[R]{
		pull: func(ctx context.Context) (R, bool, error) {
			var zero R
			item, ok, err := prev(ctx)
			if !ok || err != nil {
				return zero, false, err
			}

			new, err := itemMapper.Map(ctx, item)
			if err != nil {
				return zero, false, err
			}

			return new, true, nil
		},
	}
}
