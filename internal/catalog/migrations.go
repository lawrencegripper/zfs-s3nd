package catalog

import (
	"fmt"
	"os"
	"path/filepath"
)

const migrationsDir = "migrations"

func findMigrationsDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get cwd: %w", err)
	}
	for dir := cwd; ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, migrationsDir)
		if stat, err := os.Stat(candidate); err == nil && stat.IsDir() {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	return "", fmt.Errorf("%s directory does not exist at or above %s", migrationsDir, cwd)
}
