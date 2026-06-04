// Package logging provides a thin, structured-logging facade built on
// log/slog. It centralises level configuration so all components log
// consistently and so tests can silence output when needed.
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

var (
	mu     sync.RWMutex
	logger *slog.Logger
)

func init() {
	level := parseLevel(os.Getenv("LOG_LEVEL"))
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))
}

// parseLevel converts a textual level (debug/info/warn/error) into slog.Level.
// Unknown or empty values default to Info.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SetOutput swaps the underlying writer used by the global logger. Mainly
// useful from tests that want to silence or capture log output.
func SetOutput(w io.Writer, level slog.Level) {
	mu.Lock()
	defer mu.Unlock()
	logger = slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level}))
}

// L returns the current global logger. Safe to call from any goroutine.
func L() *slog.Logger {
	mu.RLock()
	defer mu.RUnlock()
	return logger
}

// With returns a child logger with the supplied key/value attributes.
func With(args ...any) *slog.Logger {
	return L().With(args...)
}

// Discard returns a logger that drops all output. Useful as a default in
// tests or library code paths that should never log.
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// LogWriter returns an io.Writer that forwards bytes to the global slog logger
// at Info level. Useful for capturing the stdlib log package output.
type sink struct{}

func (sink) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	L().Info(msg)
	return len(p), nil
}

// LogWriter is an io.Writer that bridges stdlib log into slog.
func LogWriter() io.Writer { return sink{} }
