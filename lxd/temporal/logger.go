package temporal

import (
	"log"
	"os"
)

type TemporalLogger struct {
	logger *log.Logger
}

// NewTemporalLogger creates new instance of TemporalLogger.
func NewTemporalLogger() *TemporalLogger {
	return &TemporalLogger{logger: log.New(os.Stdout, "", log.LstdFlags)}
}

func (l *TemporalLogger) println(level, msg string, keyvals []interface{}) {
	l.logger.Println(append([]interface{}{level, msg}, keyvals...)...)
}

// Debug writes message to the log.
func (l *TemporalLogger) Debug(msg string, keyvals ...interface{}) {
	//l.println("DEBUG", msg, keyvals)
}

// Info writes message to the log.
func (l *TemporalLogger) Info(msg string, keyvals ...interface{}) {
	l.println("INFO ", msg, keyvals)
}

// Warn writes message to the log.
func (l *TemporalLogger) Warn(msg string, keyvals ...interface{}) {
	l.println("WARN ", msg, keyvals)
}

// Error writes message to the log.
func (l *TemporalLogger) Error(msg string, keyvals ...interface{}) {
	l.println("ERROR", msg, keyvals)
}
