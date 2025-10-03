package temporal

import (
	"github.com/canonical/lxd/shared/logger"
)

type TemporalLogger struct {
	logger logger.Logger
}

// NewTemporalLogger creates new instance of TemporalLogger.
func NewTemporalLogger(l logger.Logger) *TemporalLogger {
	return &TemporalLogger{logger: l}
}

// Debug writes message to the log.
func (l *TemporalLogger) Debug(msg string, keyvals ...interface{}) {
	l.logger.Debug("Temporal: "+msg, logger.Ctx{"keyvals": keyvals})
}

// Info writes message to the log.
func (l *TemporalLogger) Info(msg string, keyvals ...interface{}) {
	l.logger.Info("Temporal: "+msg, logger.Ctx{"keyvals": keyvals})
}

// Warn writes message to the log.
func (l *TemporalLogger) Warn(msg string, keyvals ...interface{}) {
	l.logger.Warn("Temporal: "+msg, logger.Ctx{"keyvals": keyvals})
}

// Error writes message to the log.
func (l *TemporalLogger) Error(msg string, keyvals ...interface{}) {
	l.logger.Error("Temporal: "+msg, logger.Ctx{"keyvals": keyvals})
}
