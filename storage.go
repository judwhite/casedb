package main

import (
	"io"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/wal/walpb"
	"github.com/coreos/etcd/pkg/pbutil"
	"github.com/coreos/etcd/wal"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/pkg/types"
)

func readWAL(waldir string, snap walpb.Snapshot) (w *wal.WAL, id, cid types.ID, st raftpb.HardState, ents []raftpb.Entry) {
	var (
		err       error
		wmetadata []byte
	)

	repaired := false
	for {
		if w, err = wal.Open(waldir, snap); err != nil {
			plog.Fatalf("open wal error: %v", err)
		}
		if wmetadata, st, ents, err = w.ReadAll(); err != nil {
			w.Close()
			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
				plog.Fatalf("read wal error (%v) and cannot be repaired", err)
			}
			if !wal.Repair(waldir) {
				plog.Fatalf("WAL error (%v) cannot be repaired", err)
			} else {
				plog.Infof("repaired WAL error (%v)", err)
				repaired = true
			}
			continue
		}
		break
	}
	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)
	id = types.ID(metadata.NodeID)
	cid = types.ID(metadata.ClusterID)
	return
}
