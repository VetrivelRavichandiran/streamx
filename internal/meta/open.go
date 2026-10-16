// Package meta — store factory.
package meta

import (
	"fmt"

	"github.com/streamx/streamx/internal/config"
)

// Open returns a Store for the given DSN.
//   - postgres://...  → PostgresStore
//   - sqlite://<path> → SQLiteStore
func Open(dsn string) (Store, error) {
	kind, path, err := config.MustParseDBURL(dsn)
	if err != nil {
		return nil, err
	}
	switch kind {
	case "postgres":
		return OpenPostgres(dsn)
	case "sqlite":
		return OpenSQLite(path)
	default:
		return nil, fmt.Errorf("meta: unsupported db kind %q", kind)
	}
}
