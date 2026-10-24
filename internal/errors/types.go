package errors

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrTransient        error = errors.New("transient error")
	ErrPermanent        error = errors.New("permanent error")
	ErrAlreadyProcessed error = errors.New("already processed")
)

type ProcessingError struct {
	// One of the three above, this Kind field is domain specific to the
	// writers and will help see if the result of the operation
	// can be retried, quarantined, or should be a fatal error
	Kind  error
	Stage string
	Err   error
}

func (e *ProcessingError) Error() string {
	return fmt.Sprintf("%s: %s: <%s>", e.Kind.Error(), e.Stage, e.Err.Error())
}

func (e *ProcessingError) Unwrap() []error {
	return []error{e.Kind, e.Err}
}

func Classify(stage string, err error) error {
	if err == nil {
		return nil
	}

	if procError, ok := errors.AsType[*ProcessingError](err); ok {
		return procError
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	if pgError, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch pgError.Code {
		// 40P01 - Deadlock Detected
		// 40001 - Serialization failure
		// 55P03 - Lock not available
		// 57014 - Query Cancelled
		// 53300 - Too many connections
		// 57P01 - Server is shutting down
		case "40P01", "40001", "55P03", "57014", "53300", "57P01":
			return &ProcessingError{Kind: ErrTransient, Stage: stage, Err: err}
		// 23505 - Unique violation (careful if used on resources other than Instance, Instance_Character, etc.)
		case "23505":
			return &ProcessingError{Kind: ErrAlreadyProcessed, Stage: stage, Err: err}
		// 23503 - Foreign Key Violation
		// 23502 - Not null violation
		// 23514 - Check contraint violation
		case "23503", "23502", "23514":
			return &ProcessingError{Kind: ErrPermanent, Stage: stage, Err: err}
		default:
		}
	}

	// Let the error propagate
	return err
}
