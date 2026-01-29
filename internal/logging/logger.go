package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"time"
)

// LogLevel represents the severity of a log message
type LogLevel string

const (
	LevelDebug LogLevel = "debug"
	LevelInfo  LogLevel = "info"
	LevelWarn  LogLevel = "warn"
	LevelError LogLevel = "error"
	LevelFatal LogLevel = "fatal"
)

// LogFormat represents the output format for logs
type LogFormat string

const (
	FormatJSON LogFormat = "json"
	FormatText LogFormat = "text"
)

// Logger provides structured logging capabilities
type Logger struct {
	level  LogLevel
	format LogFormat
	output io.Writer
	fields map[string]interface{}
}

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Caller    string                 `json:"caller,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

// NewLogger creates a new logger instance
func NewLogger(level LogLevel, format LogFormat) *Logger {
	return &Logger{
		level:  level,
		format: format,
		output: os.Stdout,
		fields: make(map[string]interface{}),
	}
}

// WithField adds a field to the logger context
func (l *Logger) WithField(key string, value interface{}) *Logger {
	newLogger := &Logger{
		level:  l.level,
		format: l.format,
		output: l.output,
		fields: make(map[string]interface{}),
	}
	
	// Copy existing fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	
	// Add new field
	newLogger.fields[key] = value
	
	return newLogger
}

// WithFields adds multiple fields to the logger context
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	newLogger := &Logger{
		level:  l.level,
		format: l.format,
		output: l.output,
		fields: make(map[string]interface{}),
	}
	
	// Copy existing fields
	for k, v := range l.fields {
		newLogger.fields[k] = v
	}
	
	// Add new fields
	for k, v := range fields {
		newLogger.fields[k] = v
	}
	
	return newLogger
}

// WithError adds an error to the logger context
func (l *Logger) WithError(err error) *Logger {
	return l.WithField("error", err.Error())
}

// Debug logs a debug message
func (l *Logger) Debug(message string) {
	l.log(LevelDebug, message, nil)
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log(LevelDebug, fmt.Sprintf(format, args...), nil)
}

// Info logs an info message
func (l *Logger) Info(message string) {
	l.log(LevelInfo, message, nil)
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log(LevelInfo, fmt.Sprintf(format, args...), nil)
}

// Warn logs a warning message
func (l *Logger) Warn(message string) {
	l.log(LevelWarn, message, nil)
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log(LevelWarn, fmt.Sprintf(format, args...), nil)
}

// Error logs an error message
func (l *Logger) Error(message string) {
	l.log(LevelError, message, nil)
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log(LevelError, fmt.Sprintf(format, args...), nil)
}

// ErrorWithErr logs an error message with an error object
func (l *Logger) ErrorWithErr(message string, err error) {
	l.WithError(err).Error(message)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(message string) {
	l.log(LevelFatal, message, nil)
	os.Exit(1)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.log(LevelFatal, fmt.Sprintf(format, args...), nil)
	os.Exit(1)
}

// log is the internal logging function
func (l *Logger) log(level LogLevel, message string, err error) {
	// Check if this log level should be output
	if !l.shouldLog(level) {
		return
	}

	entry := LogEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Level:     string(level),
		Message:   message,
		Fields:    l.fields,
	}

	// Add caller information for errors and above
	if level == LevelError || level == LevelFatal {
		if _, file, line, ok := runtime.Caller(2); ok {
			entry.Caller = fmt.Sprintf("%s:%d", file, line)
		}
	}

	// Add error if present
	if err != nil {
		entry.Error = err.Error()
	}

	// Format and output
	var output string
	if l.format == FormatJSON {
		jsonBytes, _ := json.Marshal(entry)
		output = string(jsonBytes)
	} else {
		output = l.formatText(entry)
	}

	fmt.Fprintln(l.output, output)
}

// shouldLog determines if a message at the given level should be logged
func (l *Logger) shouldLog(level LogLevel) bool {
	levels := map[LogLevel]int{
		LevelDebug: 0,
		LevelInfo:  1,
		LevelWarn:  2,
		LevelError: 3,
		LevelFatal: 4,
	}

	return levels[level] >= levels[l.level]
}

// formatText formats a log entry as plain text
func (l *Logger) formatText(entry LogEntry) string {
	output := fmt.Sprintf("[%s] %s: %s", entry.Timestamp, entry.Level, entry.Message)

	if len(entry.Fields) > 0 {
		fieldsJSON, _ := json.Marshal(entry.Fields)
		output += fmt.Sprintf(" fields=%s", string(fieldsJSON))
	}

	if entry.Caller != "" {
		output += fmt.Sprintf(" caller=%s", entry.Caller)
	}

	if entry.Error != "" {
		output += fmt.Sprintf(" error=%s", entry.Error)
	}

	return output
}

// SetOutput sets the output writer for the logger
func (l *Logger) SetOutput(w io.Writer) {
	l.output = w
}

// SetLevel sets the log level
func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

// Global logger instance
var globalLogger *Logger

// InitGlobalLogger initializes the global logger
func InitGlobalLogger(level LogLevel, format LogFormat) {
	globalLogger = NewLogger(level, format)
}

// GetGlobalLogger returns the global logger instance
func GetGlobalLogger() *Logger {
	if globalLogger == nil {
		globalLogger = NewLogger(LevelInfo, FormatJSON)
	}
	return globalLogger
}

// Context-aware logging helpers

type loggerKey struct{}

// WithLogger adds a logger to the context
func WithLogger(ctx context.Context, logger *Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// FromContext retrieves a logger from the context
func FromContext(ctx context.Context) *Logger {
	if logger, ok := ctx.Value(loggerKey{}).(*Logger); ok {
		return logger
	}
	return GetGlobalLogger()
}

// Convenience functions using global logger

// Debug logs a debug message using the global logger
func Debug(message string) {
	GetGlobalLogger().Debug(message)
}

// Debugf logs a formatted debug message using the global logger
func Debugf(format string, args ...interface{}) {
	GetGlobalLogger().Debugf(format, args...)
}

// Info logs an info message using the global logger
func Info(message string) {
	GetGlobalLogger().Info(message)
}

// Infof logs a formatted info message using the global logger
func Infof(format string, args ...interface{}) {
	GetGlobalLogger().Infof(format, args...)
}

// Warn logs a warning message using the global logger
func Warn(message string) {
	GetGlobalLogger().Warn(message)
}

// Warnf logs a formatted warning message using the global logger
func Warnf(format string, args ...interface{}) {
	GetGlobalLogger().Warnf(format, args...)
}

// Error logs an error message using the global logger
func Error(message string) {
	GetGlobalLogger().Error(message)
}

// Errorf logs a formatted error message using the global logger
func Errorf(format string, args ...interface{}) {
	GetGlobalLogger().Errorf(format, args...)
}

// ErrorWithErr logs an error message with an error object using the global logger
func ErrorWithErr(message string, err error) {
	GetGlobalLogger().ErrorWithErr(message, err)
}

// Fatal logs a fatal message and exits using the global logger
func Fatal(message string) {
	GetGlobalLogger().Fatal(message)
}

// Fatalf logs a formatted fatal message and exits using the global logger
func Fatalf(format string, args ...interface{}) {
	GetGlobalLogger().Fatalf(format, args...)
}

// WithField adds a field to the global logger
func WithField(key string, value interface{}) *Logger {
	return GetGlobalLogger().WithField(key, value)
}

// WithFields adds multiple fields to the global logger
func WithFields(fields map[string]interface{}) *Logger {
	return GetGlobalLogger().WithFields(fields)
}

// WithError adds an error to the global logger
func WithError(err error) *Logger {
	return GetGlobalLogger().WithError(err)
}

// ParseLogLevel parses a string into a LogLevel
func ParseLogLevel(level string) LogLevel {
	switch level {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "fatal":
		return LevelFatal
	default:
		log.Printf("Unknown log level '%s', defaulting to 'info'", level)
		return LevelInfo
	}
}

// ParseLogFormat parses a string into a LogFormat
func ParseLogFormat(format string) LogFormat {
	switch format {
	case "json":
		return FormatJSON
	case "text":
		return FormatText
	default:
		log.Printf("Unknown log format '%s', defaulting to 'json'", format)
		return FormatJSON
	}
}
