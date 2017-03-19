package logrjack

import (
	"fmt"

	"strings"

	"github.com/Sirupsen/logrus"
)

type entry struct {
	entry *logrus.Entry
}

// Entry is the interface returned by NewEntry.
//
// Use AddField or AddFields (opposed to Logrus' WithField)
// to add new fields to an entry.
//
// To add source file and line information call AddCallstack.
//
// Call the Info, Warn, Error, or Fatal methods to write the log entry.
type Entry interface {
	// AddField adds a single field to the Entry.
	AddField(key string, value interface{})

	// AddFields adds a map of fields to the Entry.
	AddFields(fields map[string]interface{})

	// Info logs the Entry with a status of "info".
	Info(args ...interface{})
	// Infof logs the Entry with a status of "info".
	Infof(format string, args ...interface{})
	// Warn logs the Entry with a status of "warn".
	Warn(args ...interface{})
	// Warnf logs the Entry with a status of "warn".
	Warnf(format string, args ...interface{})
	// Error logs the Entry with a status of "error".
	Error(args ...interface{})
	// Errorf logs the Entry with a status of "error".
	Errorf(format string, args ...interface{})
	// Fatal logs the Entry with a status of "fatal" and calls os.Exit(1).
	Fatal(args ...interface{})
	// Fatalf logs the Entry with a status of "fatal" and calls os.Exit(1).
	Fatalf(format string, args ...interface{})

	// AddError adds a field "err" with the specified error.
	AddError(err error)

	String() string
}

var std *logrus.Logger

func init() {
	std = logrus.StandardLogger()
}

// NewEntry creates a new log Entry.
func NewEntry() Entry {
	e := &entry{}
	e.entry = logrus.NewEntry(std)
	return e
}

// String returns the string representation from the reader and
// ultimately the formatter.
func (e entry) String() string {
	s, err := e.entry.String()
	if err != nil {
		return fmt.Sprintf("%s - <%s>", s, err)
	}
	return s
}

// AddError adds a field "err" with the specified error.
func (e *entry) AddError(err error) {
	e.AddField("err", err)

	var cs []string
	for _, frame := range StackTrace(err) {
		cs = append(cs, fmt.Sprintf("%s:%s:%d", frame.File(), frame.Func(), frame.Line()))
	}

	if len(cs) > 0 {
		e.AddField("stacktrace", strings.Join(cs, ", "))
	}
}

// AddField adds a single field to the Entry.
func (e *entry) AddField(key string, value interface{}) {
	e.entry = e.entry.WithField(key, value)
}

// AddFields adds a map of fields to the Entry.
func (e *entry) AddFields(fields map[string]interface{}) {
	logrusFields := logrus.Fields{}
	for k, v := range fields {
		logrusFields[k] = v
	}
	e.entry = e.entry.WithFields(logrusFields)
}

// Info logs the Entry with a status of "info".
func (e *entry) Info(args ...interface{}) {
	e.entry.Info(args...)
}

// Infof logs the Entry with a status of "info".
func (e *entry) Infof(format string, args ...interface{}) {
	e.entry.Infof(format, args...)
}

// Warn logs the Entry with a status of "warn".
func (e *entry) Warn(args ...interface{}) {
	e.entry.Warn(args...)
}

// Warnf logs the Entry with a status of "warn".
func (e *entry) Warnf(format string, args ...interface{}) {
	e.entry.Warnf(format, args...)
}

// Error logs the Entry with a status of "error".
func (e *entry) Error(args ...interface{}) {
	e.entry.Error(args...)
}

// Errorf logs the Entry with a status of "error".
func (e *entry) Errorf(format string, args ...interface{}) {
	e.entry.Errorf(format, args...)
}

// Fatal logs the Entry with a status of "fatal" and calls os.Exit(1).
func (e *entry) Fatal(args ...interface{}) {
	e.entry.Fatal(args...)
}

// Fatalf logs the Entry with a status of "fatal" and calls os.Exit(1).
func (e *entry) Fatalf(format string, args ...interface{}) {
	e.entry.Fatalf(format, args...)
}
