package tracker

import "fmt"

// Logger is the minimal logging interface. Implementations must be safe for concurrent use.
// By default, if no logger is configured, no logging is performed.
type Logger interface {
	Log(string)
}

// LeveledLogger is an optional extension to the Logger interface that provides severity-based
// logging. If the configured logger implements this interface, the event tracker will use the
// appropriate level-specific methods. Otherwise, it falls back to the basic Log method.
type LeveledLogger interface {
	Logger
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

// LogLevel represents the severity of a log message.
type LogLevel int

const (
	// LogLevelDebug includes all messages (most verbose).
	LogLevelDebug LogLevel = iota
	// LogLevelInfo includes informational, warning, and error messages.
	LogLevelInfo
	// LogLevelWarn includes warning and error messages.
	LogLevelWarn
	// LogLevelError includes only error messages (least verbose).
	LogLevelError
)

// String returns the string representation of the log level.
func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelWarn:
		return "WARN"
	case LogLevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// DefaultLogger logs indexer state changes and actions to standard output using fmt formatting.
// This implementation does not support log levels.
type DefaultLogger struct{}

// Log logs to standard output using fmt formatting.
func (DefaultLogger) Log(log string) {
	fmt.Println(log)
}

// ConsoleLogger logs to standard output with severity prefixes and level filtering.
type ConsoleLogger struct {
	minLevel LogLevel
}

// NewConsoleLogger creates a new console logger that only outputs messages at or above the
// specified minimum level. For example, setting minLevel to LogLevelInfo will suppress all
// Debug messages.
func NewConsoleLogger(minLevel LogLevel) *ConsoleLogger {
	return &ConsoleLogger{minLevel: minLevel}
}

// Log logs to standard output without a level prefix (implements Logger interface).
func (l *ConsoleLogger) Log(msg string) {
	fmt.Println(msg)
}

// Debug logs a debug-level message.
func (l *ConsoleLogger) Debug(msg string) {
	if l.minLevel <= LogLevelDebug {
		fmt.Printf("[DEBUG] %s\n", msg)
	}
}

// Info logs an info-level message.
func (l *ConsoleLogger) Info(msg string) {
	if l.minLevel <= LogLevelInfo {
		fmt.Printf("[INFO]  %s\n", msg)
	}
}

// Warn logs a warning-level message.
func (l *ConsoleLogger) Warn(msg string) {
	if l.minLevel <= LogLevelWarn {
		fmt.Printf("[WARN]  %s\n", msg)
	}
}

// Error logs an error-level message.
func (l *ConsoleLogger) Error(msg string) {
	if l.minLevel <= LogLevelError {
		fmt.Printf("[ERROR] %s\n", msg)
	}
}

// NoOpLogger is a logger that discards all messages. Useful for testing or when logging
// is explicitly disabled.
type NoOpLogger struct{}

// Log discards the message.
func (NoOpLogger) Log(string) {}

// Debug discards the message.
func (NoOpLogger) Debug(string) {}

// Info discards the message.
func (NoOpLogger) Info(string) {}

// Warn discards the message.
func (NoOpLogger) Warn(string) {}

// Error discards the message.
func (NoOpLogger) Error(string) {}
