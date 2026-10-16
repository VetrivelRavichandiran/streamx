// Package logging configures structured slog logging for all STREAMX services.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Setup returns a configured slog handler. level: debug|info|warn|error.
// service is the service name (broker-1, api, worker...).
func Setup(service, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("service", service)
}

// MustSetup panics on failure — used only in main before any error handling.
func MustSetup(service, level string) *slog.Logger {
	return Setup(service, level)
}

// Errf formats an error for logging.
func Errf(err error, format string, args ...any) string {
	return fmt.Sprintf(format+": %v", append(args, err)...)
}
