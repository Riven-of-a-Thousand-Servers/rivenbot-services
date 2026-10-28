package writer

import (
	"context"
	"os"
	"testing"

	"pgcr-processing-service/internal/errors"
)

const (
	fileName = "testdata/quarantine.log"
)

type mockWriter[T any] struct {
	returnedErr error
}

func (m *mockWriter[T]) Write(ctx context.Context, item T) error {
	return m.returnedErr
}

func Test_DeadLetterWrite(t *testing.T) {
	ctx := context.Background()
	quarantineFile, err := os.Create(fileName)
	if err != nil {
		t.Fatal("Error opening quarantine file")
	}

	defer quarantineFile.Close()

	inner := mockWriter[int]{returnedErr: errors.ErrTransient}
	var syncFunc SinkableFunc[int] = func(i int) []byte {
		return []byte("hello") // This will be written to the file
	}

	sut := NewDeadLetterWriter(quarantineFile, &inner, syncFunc)
	err = sut.Write(ctx, 5)
	if err != nil {
		t.Fatalf("not expecting error, got %v", err)
	}

	quarantineFile.Sync()

	out, err := os.ReadFile(fileName)
	if err != nil {
		t.Fatal("Error reading back from the quarantine file")
	}

	if string(out) != "hello" {
		t.Fatalf("Expectd 'hello', got %s", string(out))
	}
}
