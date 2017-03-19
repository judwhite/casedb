package main

type logger interface {
	Fatalf(format string, args ...interface{})
	Infof(format string, args ...interface{})
}

var plog logger = raftLogger{}
