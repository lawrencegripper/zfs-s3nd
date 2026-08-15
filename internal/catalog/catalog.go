package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/pressly/goose/v3"
)

type Catalog struct {
	db *sql.DB
}

func Open(path string) (*Catalog, error) {
	if path == "" {
		return nil, errors.New("database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := configureSQLite(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Catalog{db: db}, nil
}

func (c *Catalog) DB() *sql.DB {
	return c.db
}

func (c *Catalog) Close() error {
	return c.db.Close()
}

func (c *Catalog) Migrate() error {
	if err := goose.SetDialect("sqlite3"); err != nil {
		return fmt.Errorf("set goose dialect: %w", err)
	}
	dir, err := findMigrationsDir()
	if err != nil {
		return err
	}
	if err := goose.Up(c.db, dir); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}

func (c *Catalog) Health(ctx context.Context) error {
	var one int
	if err := c.db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return fmt.Errorf("sqlite health query: %w", err)
	}
	if one != 1 {
		return fmt.Errorf("sqlite health query returned %d", one)
	}
	return nil
}

func configureSQLite(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA busy_timeout=5000;",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("apply %s: %w", pragma, err)
		}
	}
	return nil
}
