package main

import (
	"os"

	"fmt"

	"github.com/coreos/etcd/pkg/fileutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	log "github.com/judwhite/logrjack"
	"github.com/pkg/errors"
)

func (s *raftServer) Start() error {
	log.Info("starting raft server...")

	storage := raft.NewMemoryStorage()
	c := &raft.Config{ // was: Config{
		ID:              s.NodeID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	var peers []raft.Peer
	for i := uint64(1); i <= 3; i++ {
		if i != s.NodeID {
			peers = append(peers, raft.Peer{ID: i})
		}
	}

	if !fileutil.Exist(s.snapdir) {
		if err := os.Mkdir(s.snapdir, 0750); err != nil {
			return errors.Wrapf(err, "cannot create dir '%s' for snapshot", s.snapdir)
		}
	}

	s.Node = raft.StartNode(c, peers)
	s.raftStorage = storage

	s.wg.Add(1)
	go s.raftLoop()

	return nil
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
}

func send(messages []raftpb.Message) {
	e := log.NewEntry()
	e.AddField("context", "raft")
	e.Infof("send count:%v", len(messages))
}

func processSnapshot(snapshot raftpb.Snapshot) {
	e := log.NewEntry()
	e.AddField("context", "raft")
	e.Infof("processSnapshot confstate:%v term:%v index:%v len(data)=%v", snapshot.Metadata.ConfState, snapshot.Metadata.Term, snapshot.Metadata.Index, len(snapshot.Data))
}

func process(entry raftpb.Entry) {
	e := log.NewEntry()
	e.AddField("context", "raft")
	e.Infof("process type:%v term:%v index:%v len(data):%v", entry.Type, entry.Term, entry.Index, len(entry.Data))
}
