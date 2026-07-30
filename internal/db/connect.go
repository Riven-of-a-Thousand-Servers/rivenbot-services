package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var fs embed.FS

var (
	gooseInitErr error
	gooseOnce    sync.Once
)

func init() {
	goose.SetBaseFS(fs)
}

// Connect to Postgres Database with the required parameters
func Connect(ctx context.Context, url string) (*sql.DB, error) {
	db, err := openDB(url)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("Failed to connect to database: %w", err)
	}

	if err := initGoose(); err != nil {
		slog.Error("Failed to initialize Goose", "error", err)
		return nil, err
	}

	if err := goose.Up(db, "migrations"); err != nil {
		slog.Error("Failed to apply migrations", "error", err)
		return nil, err
	}

	return db, nil
}

func initGoose() error {
	gooseOnce.Do(func() {
		goose.SetBaseFS(fs)
		gooseInitErr = goose.SetDialect("pgx")
	})
	return gooseInitErr
}
