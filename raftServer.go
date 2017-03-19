package main

import (
	"os"

	"fmt"

	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/snap"
	"github.com/coreos/etcd/wal/walpb"
	log "github.com/judwhite/logrjack"
	"github.com/pkg/errors"
)

type raftServer struct {
	NodeID      uint64
	Node        raft.Node
	Ticker      *time.Ticker
	raftStorage *raft.MemoryStorage

	snapdir string
	waldir  string

	done chan struct{}
	wg   sync.WaitGroup
}

func newRaftServer(id uint64) *raftServer {
	return &raftServer{
		NodeID:  id,
		Ticker:  time.NewTicker(50 * time.Millisecond),
		snapdir: "snapshots",
		waldir:  "wal",
		done:    make(chan struct{}),
	}
}

func (s *raftServer) Start() error {
	log.Info("starting raft server...")

	storage := raft.NewMemoryStorage()
	s.raftStorage = storage

	c := &raft.Config{
		ID:              s.NodeID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if !exists(s.snapdir) {
		if err := os.Mkdir(s.snapdir, 0750); err != nil {
			return errors.Wrapf(err, "cannot create dir '%s' for snapshot", s.snapdir)
		}
	}

	if !exists(s.waldir) {
		if err := os.Mkdir(s.waldir, 0750); err != nil {
			return errors.Wrapf(err, "cannot create dir '%s' for wal", s.waldir)
		}
	}

	var snapshot *raftpb.Snapshot
	ss := snap.New(s.snapdir)
	snapshot, err := ss.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		plog.Fatalf("ERR: %T %v", err, err)
	}
	if snapshot != nil {
		// Restart cluster from a previous start
		var walsnap walpb.Snapshot
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
		_, _, _, state, entries := readWAL(s.waldir, walsnap)
		storage.ApplySnapshot(*snapshot)
		storage.SetHardState(state)
		storage.Append(entries)
		s.Node = raft.RestartNode(c)
	} else {
		// Start a new cluster
		var peers []raft.Peer
		for i := uint64(1); i <= 3; i++ {
			if i != s.NodeID {
				peers = append(peers, raft.Peer{ID: i})
			}
		}

		s.Node = raft.StartNode(c, peers)
	}

	// Start Raft loop
	s.wg.Add(1)
	go s.raftLoop()

	return nil
}

func exists(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

func (s *raftServer) Stop() {
	log.Info("stopping raft server...")
	s.Node.Stop()
	close(s.done)
	s.wg.Wait()
}

func (s *raftServer) raftLoop() {
	defer func() {
		if perr := recover(); perr != nil {
			perror, ok := perr.(error)
			if !ok {
				perror = fmt.Errorf("%v", perr)
			}
			perror = errors.Wrap(perror, "panic in raftLoop")

			e := log.NewEntry()
			e.AddError(perror)
			e.Fatal()
		}
	}()

	for {
		select {
		case <-s.Ticker.C: // was: s.Ticker
			s.Node.Tick() // was: n.Tick
		case rd := <-s.Node.Ready():
			saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
			send(rd.Messages)
			if !raft.IsEmptySnap(rd.Snapshot) {
				s.raftStorage.ApplySnapshot(rd.Snapshot)
				processSnapshot(rd.Snapshot)
			}
			s.raftStorage.Append(rd.Entries)
			for _, entry := range rd.CommittedEntries {
				process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					s.Node.ApplyConfChange(cc)
				}
			}
			s.Node.Advance()
		case <-s.done:
			s.wg.Done()
			return
		}
	}
}

func saveToStorage(state raftpb.HardState, entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	e := log.NewEntry()
	e.AddField("context", "raft")
	e.Infof("saveToStorage state:%#v len(entries):%v len(snapshot):%v", state, len(entries), len(snapshot.Data))

	// TODO (judwhite): finish implementation
}

func send(messages []raftpb.Message) {
	e := log.NewEntry()
	e.AddField("context", "raft")
	e.Infof("send count:%v", len(messages))

	// TODO (judwhite): finish implementation
}

func processSnapshot(snapshot raftpb.Snapshot) {
	e := log.NewEntry()
	e.AddField("context", "raft")
	e.Infof("processSnapshot confstate:%v term:%v index:%v len(data)=%v", snapshot.Metadata.ConfState, snapshot.Metadata.Term, snapshot.Metadata.Index, len(snapshot.Data))

	// TODO (judwhite): finish implementation
}

func process(entry raftpb.Entry) {
	e := log.NewEntry()
	e.AddField("context", "raft")
	e.Infof("process type:%v term:%v index:%v len(data):%v", entry.Type, entry.Term, entry.Index, len(entry.Data))

	// TODO (judwhite): finish implementation
}
