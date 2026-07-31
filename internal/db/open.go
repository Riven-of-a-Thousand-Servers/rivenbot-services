package db

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

var dataDirectory = "/data"

func detectDriver(dsn string) (driver string, connStr string, err error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", fmt.Errorf("Invalid DSN: %v", err)
	}

	switch strings.ToLower(u.Scheme) {
	case "postgres", "postgresql":
		return "pgx", dsn, nil
	case "sqlite", "sqlite3":
		err := os.MkdirAll(dataDirectory, 0o755)
		if err != nil {
			return "", "", err
		}
		return "sqlite", strings.TrimPrefix(dsn, "sqlite://"), nil
	default:
		return "", "", fmt.Errorf("Unsupported driver: %s", u.Scheme)
	}
}

func openDB(url string) (*sql.DB, error) {
	driver, connUrl, err := detectDriver(url)
	if err != nil {
		return nil, err
	}

	return sql.Open(driver, connUrl)
}
