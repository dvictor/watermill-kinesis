package kinesis

import (
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/vmware/vmware-go-kcl-v2/logger"
	"io"
	"log"
	"os"
	"sort"
	"strings"
)

type loggerAdapter struct {
	log watermill.LoggerAdapter
}

func (l *loggerAdapter) Debugf(format string, args ...interface{}) {
	l.log.Debug(fmt.Sprintf(format, args...), nil)
}

func (l *loggerAdapter) Infof(format string, args ...interface{}) {
	l.log.Info(fmt.Sprintf(format, args...), nil)
}

func (l *loggerAdapter) Warnf(format string, args ...interface{}) {
	l.log.Info(fmt.Sprintf(format, args...), watermill.LogFields{"lvl": "warn"})
}

func (l *loggerAdapter) Errorf(format string, args ...interface{}) {
	l.log.Error("", fmt.Errorf(format, args...), nil)
}

func (l *loggerAdapter) Fatalf(format string, args ...interface{}) {
	l.Errorf(format, args...)
	os.Exit(1)
}

func (l *loggerAdapter) Panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func (l *loggerAdapter) WithFields(keyValues logger.Fields) logger.Logger {
	return &loggerAdapter{
		log: l.log.With(watermill.LogFields(keyValues)),
	}
}

func convertLogger(log watermill.LoggerAdapter) logger.Logger {
	return &loggerAdapter{log}
}

type StdLogger struct {
	errLogger   *log.Logger
	infoLogger  *log.Logger
	debugLogger *log.Logger
	traceLogger *log.Logger

	fields watermill.LogFields

	prefix string
}

func (l *StdLogger) Error(msg string, err error, fields watermill.LogFields) {
	l.log(l.errLogger, "ERROR", msg, fields.Add(watermill.LogFields{"err": err}))
}

func (l *StdLogger) Info(msg string, fields watermill.LogFields) {
	l.log(l.infoLogger, "INFO", msg, fields)
}

func (l *StdLogger) Debug(msg string, fields watermill.LogFields) {
	l.log(l.debugLogger, "DEBUG", msg, fields)
}

func (l *StdLogger) Trace(msg string, fields watermill.LogFields) {
	l.log(l.traceLogger, "TRACE", msg, fields)
}

func (l *StdLogger) With(fields watermill.LogFields) watermill.LoggerAdapter {
	return &StdLogger{
		errLogger:   l.errLogger,
		infoLogger:  l.infoLogger,
		debugLogger: l.debugLogger,
		traceLogger: l.traceLogger,
		fields:      l.fields.Add(fields),
		prefix:      l.prefix,
	}
}

func (l *StdLogger) WithDebug(debug bool) *StdLogger {
	ret := &StdLogger{
		errLogger:   l.errLogger,
		infoLogger:  l.infoLogger,
		traceLogger: l.traceLogger,
		fields:      l.fields,
		prefix:      l.prefix,
	}
	if debug {
		ret.debugLogger = l.errLogger
	}
	return ret
}

func (l *StdLogger) WithPrefix(prefix string) *StdLogger {
	return &StdLogger{
		errLogger:   l.errLogger,
		infoLogger:  l.infoLogger,
		debugLogger: l.debugLogger,
		traceLogger: l.traceLogger,
		fields:      l.fields,
		prefix:      prefix,
	}
}

func (l *StdLogger) log(logger *log.Logger, level string, msg string, fields watermill.LogFields) {
	if logger == nil {
		return
	}

	fieldsStr := ""

	allFields := l.fields.Add(fields)

	keys := make([]string, len(allFields))
	i := 0
	for field := range allFields {
		keys[i] = field
		i++
	}

	sort.Strings(keys)

	for _, key := range keys {
		var valueStr string
		value := allFields[key]

		if stringer, ok := value.(fmt.Stringer); ok {
			valueStr = stringer.String()
		} else {
			valueStr = fmt.Sprintf("%v", value)
		}

		if strings.Contains(valueStr, " ") {
			valueStr = `"` + valueStr + `"`
		}

		fieldsStr += key + "=" + valueStr + " "
	}

	_ = logger.Output(0, fmt.Sprintf(`%slevel=%s msg="%s" %s`, l.prefix, level, msg, fieldsStr))
}

// NewLogger creates a Watermill logger that prints to Stdout
// This is different from  watermill.NewLogger in that it does not print the caller file/line.
// Due to the log adapter used for the KCL library, the file location is incorrect.
func NewLogger(out io.Writer, info, debug, trace bool) *StdLogger {
	l := log.New(out, "", 0)
	ret := &StdLogger{
		errLogger: l,
	}
	if info {
		ret.infoLogger = l
	}
	if debug {
		ret.debugLogger = l
	}
	if trace {
		ret.traceLogger = l
	}
	return ret
}

func NewStdLogger(info, debug, trace bool) *StdLogger {
	return NewLogger(os.Stderr, info, debug, trace)
}
