package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/judwhite/go-svc/svc"
	"github.com/judwhite/httplog"
	log "github.com/judwhite/logrjack"
)

type program struct {
	webServer *webServer
}

type webServer struct {
	httplog.Server
	raftServer *raftServer
}

type raftServer struct {
	NodeID      uint64
	Node        raft.Node
	Ticker      *time.Ticker
	raftStorage *raft.MemoryStorage

	snapdir string

	done chan struct{}
	wg   sync.WaitGroup
}

var settings appSettings

func main() {
	id := flag.Uint64("nodeid", 0, "node id")

	flag.Parse()

	if *id == 0 {
		flag.PrintDefaults()
		fmt.Println("nodeid must be >= 1")
		os.Exit(1)
	}

	settings.HTTPListenAddress = fmt.Sprintf(":62%02d", *id)

	prg := program{
		webServer: &webServer{
			Server: httplog.Server{
				FormatJSON:      true,
				ShutdownTimeout: 2 * time.Second,
				NewLogEntry:     func() httplog.Entry { return log.NewEntry() },
			},
			raftServer: &raftServer{
				NodeID:  *id,
				Ticker:  time.NewTicker(50 * time.Millisecond),
				snapdir: "snapshots",
				done:    make(chan struct{}),
			},
		},
	}
	if err := svc.Run(&prg); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Init(env svc.Environment) error {
	isWindowsService := env.IsWindowsService()

	// TODO (judwhite): allow specifying log filename

	log.Setup(log.Settings{
		WriteStdout: !isWindowsService,
		MaxAgeDays:  15,
		MaxSizeMB:   5,
		MaxBackups:  50,
	})

	raft.SetLogger(raftLogger{})

	return nil
}

func (p *program) Start() error {
	log.Info("program starting...")
	if err := p.webServer.Start(); err != nil {
		return err
	}
	log.Info("program started")
	return nil
}

func (p *program) Stop() error {
	log.Info("program stopping...")
	p.webServer.Stop()
	log.Info("program stopped")
	return nil
}

type raftLogger struct{}

func (_ raftLogger) NewEntry() log.Entry {
	e := log.NewEntry()
	e.AddField("module", "coreos/etcd/raft")
	return e
}

func (l raftLogger) Debug(v ...interface{})                   { l.NewEntry().Info(v) }
func (l raftLogger) Debugf(format string, v ...interface{})   { l.NewEntry().Infof(format, v...) }
func (l raftLogger) Info(v ...interface{})                    { l.NewEntry().Info(v) }
func (l raftLogger) Infof(format string, v ...interface{})    { l.NewEntry().Infof(format, v...) }
func (l raftLogger) Warning(v ...interface{})                 { l.NewEntry().Warn(v) }
func (l raftLogger) Warningf(format string, v ...interface{}) { l.NewEntry().Warnf(format, v...) }
func (l raftLogger) Error(v ...interface{})                   { l.NewEntry().Error(v) }
func (l raftLogger) Errorf(format string, v ...interface{})   { l.NewEntry().Errorf(format, v...) }
func (l raftLogger) Fatal(v ...interface{})                   { l.NewEntry().Fatal(v) }
func (l raftLogger) Fatalf(format string, v ...interface{})   { l.NewEntry().Fatalf(format, v...) }
func (l raftLogger) Panic(v ...interface{})                   { l.NewEntry().Fatal(v) }
func (l raftLogger) Panicf(format string, v ...interface{})   { l.NewEntry().Fatalf(format, v...) }
