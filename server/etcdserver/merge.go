package etcdserver

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/google/renameio"
	"github.com/google/uuid"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/measure"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	StoreKey = "tx-store"
)

type phaseEnum uint8

const (
	prepareYes phaseEnum = iota
	prepareNo
	committed
	aborted

	ackedCommit
	ackedAbort
)

type txState struct {
	Phase    phaseEnum
	Progress map[types.ID]struct{}
	Clusters map[types.ID][]pb.Member
	Info     raftpb.MergeInfo
}

// TODO: improve by keep an in-memory buffer
type txStore struct {
	Ongoing uuid.UUID
	States  map[uuid.UUID]txState
}

type merger struct {
	id        types.ID
	rn        raft.Node
	kv        mvcc.KV
	ticker    *time.Ticker
	dataDir   string
	applierV3 applierV3
	cluster   *membership.RaftCluster
	transport rafthttp.Transporter
	Lessor    lease.Lessor
	msgChan   chan raftpb.Message
	sendChan  chan struct{}
	snapChan  chan struct{}

	lg *zap.Logger
}

func (m *merger) getTxStore(ctx context.Context) txStore {
	txn := m.kv.Write(traceutil.Get(context.Background()))
	defer txn.End()

	rr, err := txn.Range(ctx, []byte(StoreKey), nil, mvcc.RangeOptions{})
	if err != nil {
		panic(fmt.Errorf("cannot read tx store: %v", err))
	}

	var store txStore
	if len(rr.KVs) != 0 {
		if err = gob.NewDecoder(bytes.NewBuffer(rr.KVs[0].Value)).Decode(&store); err != nil {
			panic(fmt.Errorf("cannot decode tx store: %v", err))
		}
	} else {
		store = txStore{Ongoing: uuid.Nil, States: map[uuid.UUID]txState{}}
	}

	return store
}

func (m *merger) putTxStore(store txStore) {
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(store); err != nil {
		panic(fmt.Errorf("encode store failed: %v", err))
	}

	txn := m.kv.Write(traceutil.Get(context.Background()))
	defer txn.End()
	txn.Put([]byte(StoreKey), buf.Bytes(), lease.NoLease)
}

func (m *merger) proposeMerge(ctx context.Context, r pb.MemberMergeRequest) error {
	clusters := make([]raftpb.Cluster, 0, len(r.Clusters))

	// add this cluster (coordinator) in if not in the request
	if _, ok := r.Clusters[uint64(m.cluster.ID())]; !ok {
		membersBuf := make([][]byte, 0, len(m.cluster.Members()))
		for _, mem := range m.cluster.Members() {
			membersBuf = append(membersBuf,
				pbutil.MustMarshal(&pb.Member{
					ID:         uint64(mem.ID),
					Name:       mem.Name,
					PeerURLs:   mem.PeerURLs,
					ClientURLs: mem.ClientURLs,
				}))
		}
		clusters = append(clusters, raftpb.Cluster{Id: uint64(m.cluster.ID()), Members: membersBuf})
	}

	// add clusters in the request
	for cid, members := range r.Clusters {
		membersBuf := make([][]byte, 0, len(members.Members))
		for _, mem := range members.Members {
			membersBuf = append(membersBuf, pbutil.MustMarshal(&mem))
		}

		clusters = append(clusters, raftpb.Cluster{Id: cid, Members: membersBuf})
	}

	measure.Update() <- measure.Measure{MergeTxIssue: measure.Time(time.Now())}

	return m.rn.ProposeMerge(ctx,
		raftpb.MergeInfo{
			Type:        raftpb.Prepare,
			Txid:        uuid.New().String(),
			Coordinator: uint64(m.cluster.ID()),
			Clusters:    clusters,
		})
}

func validateInfo(info raftpb.MergeInfo, store txStore) error {
	if info.Type == raftpb.Prepare || info.Type == raftpb.Prepared {
		return nil
	}

	txid := uuid.MustParse(info.Txid)
	state, ok := store.States[txid]
	if !ok {
		return fmt.Errorf("tx %v not exists but to %v",
			txid, strings.ToLower(info.Type.String()))
	}

	switch info.Type {
	case raftpb.Commit:
		if state.Phase == prepareNo || state.Phase == aborted || state.Phase == ackedAbort {
			return fmt.Errorf("tx %v should abort but to commit", txid)
		}
	case raftpb.Abort:
		if state.Phase == committed || state.Phase == ackedCommit {
			return fmt.Errorf("tx %v should commit but to abort", txid)
		}
	}
	return nil
}

func duplicateInfo(info raftpb.MergeInfo, store txStore) bool {
	txid := uuid.MustParse(info.Txid)
	state, ok := store.States[txid]
	switch info.Type {
	case raftpb.Prepare:
		return ok
	case raftpb.Prepared:
		_, ok = state.Progress[types.ID(info.Clusters[0].Id)]
		return ok
	case raftpb.Commit:
		return state.Phase == committed || state.Phase == ackedCommit
	case raftpb.Abort:
		return state.Phase == aborted || state.Phase == ackedAbort
	case raftpb.Ack:
		return state.Phase == ackedCommit || state.Phase == ackedAbort
	default:
		panic(fmt.Sprintf("unknown info type: %v", info.Type))
	}
}

func (m *merger) apply(entry raftpb.Entry) {
	store := m.getTxStore(context.Background())

	var info raftpb.MergeInfo
	pbutil.MustUnmarshal(&info, entry.Data)

	if err := validateInfo(info, store); err != nil {
		panic(fmt.Errorf("invalid info: %v", err))
	}

	if duplicateInfo(info, store) {
		m.lg.Debug("ignore duplicate tx entry",
			zap.String("txid", info.Txid),
			zap.String("type", info.Type.String()))
		return
	}

	sendNow := false

	txid := uuid.MustParse(info.Txid)
	switch info.Type {
	case raftpb.Prepare: // on both coordinator and participants
		// TODO: detect configuration change
		// reject concurrent tx
		if store.Ongoing != uuid.Nil {
			store.States[txid] = txState{Phase: prepareNo, Info: info}
			break
		}

		// parse clusters
		clusters := make(map[types.ID][]pb.Member)
		for _, clr := range info.Clusters {
			cid := types.ID(clr.Id)
			clusters[cid] = make([]pb.Member, 0, len(clr.Members))
			for _, memberBuf := range clr.Members {
				var member pb.Member
				pbutil.MustUnmarshal(&member, memberBuf)
				clusters[cid] = append(clusters[cid], member)

				m.transport.AddPeer(types.ID(member.ID), member.PeerURLs)
			}
		}

		// initial status
		txs := txState{
			Phase:    prepareYes,
			Clusters: clusters,
			Progress: make(map[types.ID]struct{}, len(clusters)),
			Info:     info,
		}
		txs.Progress[m.cluster.ID()] = struct{}{}

		// store
		store.States[txid] = txs
		store.Ongoing = txid

		sendNow = true

		measure.Update() <- measure.Measure{MergeTxStart: measure.Time(time.Now())}
		m.lg.Debug("start new merge tx",
			zap.String("txid", txid.String()),
			zap.Any("clusters", txs.Clusters))

	case raftpb.Prepared: // only on coordinator
		txs := store.States[txid]
		cid := types.ID(info.Clusters[0].Id)
		txs.Progress[cid] = struct{}{}
		m.lg.Debug("committed cluster",
			zap.String("txid", txid.String()),
			zap.Any("cluster", cid))

		if len(txs.Progress) == len(txs.Clusters) {
			txs.Phase = committed
			txs.Progress = make(map[types.ID]struct{}, len(txs.Clusters))
			txs.Progress[m.cluster.ID()] = struct{}{}
			txs.Info.Type = raftpb.Commit

			if txs.Info.Coordinator != uint64(m.cluster.ID()) {
				m.applyMergeConfChange(txs.Clusters, info)
				store.Ongoing = uuid.Nil
			}

			sendNow = true

			measure.Update() <- measure.Measure{MergeTxCommit: measure.Time(time.Now())}
			m.lg.Debug("coordinator committed merge tx",
				zap.String("txid", txid.String()),
				zap.Any("phase", txs.Phase),
				zap.Any("clusters", txs.Clusters))
		}
		store.States[txid] = txs

	case raftpb.Commit: // only on participants
		txs := store.States[txid]

		txs.Phase = committed
		txs.Progress = make(map[types.ID]struct{}, len(txs.Clusters))
		txs.Progress[m.cluster.ID()] = struct{}{}
		txs.Info.Type = raftpb.Commit

		m.applyMergeConfChange(txs.Clusters, info)

		sendNow = true

		measure.Update() <- measure.Measure{MergeTxCommit: measure.Time(time.Now())}
		m.lg.Debug("participant committed merge tx",
			zap.String("txid", txid.String()),
			zap.Any("phase", txs.Phase),
			zap.Any("clusters", txs.Clusters))

		store.States[txid] = txs
		store.Ongoing = uuid.Nil

	case raftpb.Abort:
		txs := store.States[txid]

		txs.Phase = aborted
		txs.Progress = make(map[types.ID]struct{}, len(txs.Clusters))
		txs.Progress[m.cluster.ID()] = struct{}{}
		txs.Info = info
		store.States[txid] = txs

		sendNow = true

		m.lg.Debug("abort merge tx",
			zap.String("txid", txid.String()),
			zap.Any("clusters", txs.Clusters))

	case raftpb.Ack:
		txs := store.States[txid]
		cid := types.ID(info.Clusters[0].Id)
		txs.Progress[cid] = struct{}{}
		m.lg.Debug("acked cluster",
			zap.String("txid", txid.String()),
			zap.Any("phase", txs.Phase),
			zap.Any("cluster", cid))

		if len(txs.Progress) == len(txs.Clusters) {
			if txs.Phase == committed {
				txs.Phase = ackedCommit
			} else if txs.Phase == aborted {
				txs.Phase = ackedAbort
			} else {
				panic(fmt.Sprintf("invalid state %v to ack tx %v", txs.Phase, txid))
			}

			if txs.Info.Coordinator != uint64(m.cluster.ID()) {
				panic("ack commit by a non-coordinator cluster")
			}
			m.applyMergeConfChange(txs.Clusters, txs.Info)

			txs.Info.Type = raftpb.Ack
			store.Ongoing = uuid.Nil
			store.States[txid] = txs

			m.lg.Debug("acked merge tx",
				zap.String("txid", txid.String()),
				zap.Any("phase", txs.Phase),
				zap.Any("clusters", txs.Clusters))
		}
	}

	m.putTxStore(store)
	if sendNow {
		go func() { m.sendChan <- struct{}{} }()
	}
}

func (m *merger) applyMergeConfChange(clusters map[types.ID][]pb.Member, info raftpb.MergeInfo) {
	m.lg.Debug("ready to apply merge", zap.String("txid", info.Txid))

	// resume after requested all snapshots
	m.ticker.Stop()

	// add to cluster membership
	changes := make([]raftpb.ConfChangeSingle, 0)
	for cid, members := range clusters {
		if cid == m.cluster.ID() {
			continue
		}
		for _, mem := range members {
			m.cluster.AddMember(&membership.Member{
				ID: types.ID(mem.ID),
				RaftAttributes: membership.RaftAttributes{
					PeerURLs:  mem.PeerURLs,
					IsLearner: false,
				},
				Attributes: membership.Attributes{
					Name:       mem.Name,
					ClientURLs: mem.ClientURLs,
				},
			}, membership.ApplyBoth)

			changes = append(changes, raftpb.ConfChangeSingle{
				Type:    raftpb.ConfChangeMergeNode,
				NodeID:  mem.ID,
				Context: []byte(strconv.FormatUint(uint64(cid), 10)),
			})
		}
	}

	// add to raft membership
	m.rn.ApplyConfChange(raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionMerge,
		Changes:    changes,
		Context:    pbutil.MustMarshal(&info),
	})
	m.lg.Debug("applied merge", zap.String("txid", info.Txid))

	// take a snapshot of Data
	go m.snapshotter()

	// start a snapshot requester
	go m.snapshotRequester(clusters)
}

type MergeSnapshot []mvccpb.KeyValue

func (m *merger) snapFilepath(clusterId types.ID) string {
	return m.dataDir + "/" + "merge-" + clusterId.String() + ".snap"
}

func (m *merger) isSnapFileExists(clusterId types.ID) bool {
	_, err := os.Stat(m.snapFilepath(clusterId))
	return err == nil || !os.IsNotExist(err)
}

func (m *merger) mustReadSnapFile(clusterId types.ID) []byte {
	data, err := os.ReadFile(m.snapFilepath(clusterId))
	if err != nil {
		panic(fmt.Errorf("read snap file %s failed %v", clusterId, err))
	}
	return data
}

func (m *merger) mustWriteSnapFile(clusterId types.ID, data []byte) {
	if err := renameio.WriteFile(m.snapFilepath(clusterId), data, 0644); err != nil {
		panic(err)
	}
}

func (m merger) snapshotter() {
	txn := m.kv.Read(mvcc.ConcurrentReadTxMode, traceutil.Get(context.Background()))
	rr1, err := txn.Range(context.Background(), []byte{0}, []byte(StoreKey), mvcc.RangeOptions{})
	rr2, err := txn.Range(context.Background(), []byte(StoreKey+"\x00"), []byte{}, mvcc.RangeOptions{})
	if err != nil {
		txn.End()
		panic(fmt.Errorf("cannot read kv for snapshot: %v", err))
	}
	txn.End()

	buf := bytes.Buffer{}
	if err = gob.NewEncoder(&buf).Encode(MergeSnapshot(append(rr1.KVs, rr2.KVs...))); err != nil {
		panic(fmt.Errorf("encode snapshot kv failed: %v", err))
	}
	m.mustWriteSnapFile(m.cluster.ID(), buf.Bytes())
}

func (m merger) snapshotRequester(clusters map[types.ID][]pb.Member) {
	exists := make(map[types.ID]struct{})
	exists[m.cluster.ID()] = struct{}{}
	for len(exists) != len(clusters) {
		for cid, mems := range clusters {
			if _, ok := exists[cid]; ok {
				continue
			}

			if m.isSnapFileExists(cid) {
				exists[cid] = struct{}{}
			} else {
				reqs := make([]raftpb.Message, 0)
				for _, member := range mems {
					reqs = append(reqs, raftpb.Message{
						Type: raftpb.MsgMergeSnapReq,
						To:   member.ID,
						From: uint64(m.id),
					})
					m.lg.Debug("request merge snap", zap.Stringer("Cid", cid))
				}

				m.transport.Send(reqs)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (m *merger) enterJoint(cc raftpb.ConfChangeV2) {
	m.lg.Debug("ready to enter merge joint")
	m.rn.ApplyConfChange(cc)
	m.lg.Debug("enter merge joint")
}

func (m *merger) run() {
	store := m.getTxStore(context.Background())
	if store.Ongoing != uuid.Nil {
		// Should only happen during reboot,
		// the backend restarts with already most up-to-date Data
		m.lg.Debug("restore merge tx", zap.String("txid", store.Ongoing.String()))
		for _, clr := range store.States[store.Ongoing].Clusters {
			for _, member := range clr {
				m.transport.AddPeer(types.ID(member.ID), member.PeerURLs)
			}
		}
	}

	go m.sender()
	go m.handler()
}

func (m *merger) sender() {
	// ticking and sending messages out for ongoing tx
	for {
		select {
		case <-m.sendChan:
		case <-time.After(25 * time.Millisecond):
		}

		if m.rn.Status().SoftState.Lead != uint64(m.id) {
			continue
		}

		store := m.getTxStore(context.Background())
		if store.Ongoing != uuid.Nil && store.States[store.Ongoing].Info.Coordinator == uint64(m.cluster.ID()) {
			txid := store.Ongoing
			txs := store.States[txid]

			msgs := make([]raftpb.Message, 0)
			for cid, clr := range txs.Clusters {
				if cid == m.cluster.ID() {
					continue
				}
				for _, member := range clr {
					msg := raftpb.Message{From: uint64(m.id), To: member.ID}
					switch txs.Phase {
					case prepareYes:
						msg.Type = raftpb.MsgMergePrepare
						msg.Context = pbutil.MustMarshal(&txs.Info)
					case committed:
						msg.Type = raftpb.MsgMergeCommit
						msg.Context = []byte(txid.String())
					case aborted:
						msg.Type = raftpb.MsgMergeAbort
						msg.Context = []byte(txid.String())
					default:
						panic(fmt.Sprintf("impossible txs phase: %v", txs.Phase))
					}

					m.lg.Debug("send tx message",
						zap.Stringer("type", msg.Type),
						zap.Uint64("to", msg.To),
						zap.Stringer("txid", txid))
					msgs = append(msgs, msg)
				}
			}

			m.transport.Send(msgs)
		}
	}
}

type snapshotResp struct {
	Cid  types.ID
	Data []byte
}

func (m *merger) handler() {
	for {
		msg := <-m.msgChan
		m.lg.Debug("receive tx message",
			zap.Stringer("type", msg.Type),
			zap.Uint64("from", msg.From))

		if msg.Type == raftpb.MsgMergeSnapReq {
			m.lg.Debug("receive merge snap request", zap.Uint64("from", msg.From))

			if m.isSnapFileExists(m.cluster.ID()) {
				data := m.mustReadSnapFile(m.cluster.ID())
				buf := bytes.Buffer{}
				if err := gob.NewEncoder(&buf).Encode(snapshotResp{
					Cid:  m.cluster.ID(),
					Data: data,
				}); err != nil {
					m.lg.Panic("encode snapshot resp failed", zap.Error(err))
					continue
				}

				m.transport.Send([]raftpb.Message{
					{
						Type:    raftpb.MsgMergeSnapResp,
						To:      msg.From,
						From:    msg.To,
						Context: buf.Bytes(),
					},
				})
			}
			continue
		} else if msg.Type == raftpb.MsgMergeSnapResp {
			m.lg.Debug("receive merge snap response", zap.Uint64("from", msg.From))

			var resp snapshotResp
			if err := gob.NewDecoder(bytes.NewBuffer(msg.Context)).Decode(&resp); err != nil {
				m.lg.Panic("handle snapshot resp failed", zap.Error(err))
			}

			if !m.isSnapFileExists(resp.Cid) {
				m.mustWriteSnapFile(resp.Cid, resp.Data)
			}
			continue
		}

		if m.rn.Status().Lead != raft.None && m.rn.Status().Lead != uint64(m.id) {
			continue
		}

		store := m.getTxStore(context.Background())

		var txid uuid.UUID
		var state txState
		var info raftpb.MergeInfo
		if msg.Type == raftpb.MsgMergePrepare {
			pbutil.MustUnmarshal(&info, msg.Context)
			txid = uuid.MustParse(info.Txid)
		} else {
			txid = uuid.MustParse(string(msg.Context))
			s, ok := store.States[txid]
			if !ok {
				panic(fmt.Sprintf("receive message type %v for tx %v but state not found", msg.Type, txid))
			}
			if state.Phase == ackedCommit || state.Phase == ackedAbort {
				m.lg.Debug("ignore acked tx", zap.Stringer("txid", txid))
			}
			state = s
		}
		m.lg.Debug("tx message", zap.Stringer("txid", txid))

		resps := make([]raftpb.Message, 0)
		switch msg.Type {
		case raftpb.MsgMergePrepare: // on participants
			// validation and duplication check
			state, ok := store.States[txid]
			if ok {
				m.lg.Debug("duplicate tx message",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))

				var resp raftpb.Message
				if state.Phase == prepareYes || state.Phase == committed {
					resp = raftpb.Message{Type: raftpb.MsgMergePrepareYes, From: msg.To, To: msg.From,
						Context: []byte(txid.String())}
				} else if state.Phase == prepareNo || state.Phase == aborted {
					resp = raftpb.Message{Type: raftpb.MsgMergePrepareNo, From: msg.To, To: msg.From,
						Context: []byte(txid.String())}
				}
				resps = append(resps, resp)
				break
			}

			// propose new tx
			m.lg.Debug("ready to prepare tx", zap.Any("info", info))
			if err := m.rn.ProposeMerge(context.Background(), info); err != nil {
				m.lg.Debug("propose to prepare tx failed", zap.Error(err))
			}

		case raftpb.MsgMergeCommit: // on participants
			// validation and duplication check
			if state.Phase == prepareNo || state.Phase == aborted {
				panic(fmt.Sprintf("tx %v should abort (%v) but to commit", txid, state.Phase))
			} else if state.Phase == committed {
				m.lg.Debug("duplicate tx message",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))
				resps = append(resps, raftpb.Message{
					Type:    raftpb.MsgMergeAck,
					To:      msg.From,
					From:    msg.To,
					Context: []byte(txid.String()),
				})
				break
			}

			state.Info.Type = raftpb.Commit
			m.lg.Debug("ready to commit tx", zap.Stringer("txid", txid))
			if err := m.rn.ProposeMerge(context.Background(), state.Info); err != nil {
				m.lg.Debug("propose to commit tx failed", zap.Error(err))
			}

		case raftpb.MsgMergeAbort: // on participants
			// validation and duplication check
			if state.Phase == committed {
				panic(fmt.Sprintf("tx %v should commit (%v) but to abort", txid, state.Phase))
			} else if state.Phase == aborted {
				m.lg.Debug("duplicate tx message",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))
				resps = append(resps, raftpb.Message{
					Type:    raftpb.MsgMergeAck,
					To:      msg.From,
					From:    msg.To,
					Context: []byte(txid.String()),
				})
				break
			}

			state.Info.Type = raftpb.Abort
			m.lg.Debug("ready to abort tx", zap.Stringer("txid", txid))
			if err := m.rn.ProposeMerge(context.Background(), state.Info); err != nil {
				m.lg.Debug("propose to abort tx failed", zap.Error(err))

			}

		case raftpb.MsgMergePrepareYes: // on coordinator
			// validation and duplication check
			if state.Phase != prepareYes {
				m.lg.Debug("duplicate tx message",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))
				break
			}

			// find cluster id of sender
			var cid types.ID
			for clr, mems := range state.Clusters {
				for _, member := range mems {
					if member.ID == msg.From {
						cid = clr
					}
				}
			}
			if cid == 0 {
				m.lg.Debug("unknown Cid",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))
				break
			}

			if _, ok := state.Progress[cid]; ok {
				m.lg.Debug("duplicate tx message",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))
				zap.Stringer("cluster", cid)
				break
			}

			m.lg.Debug("ready to propose cluster prepare",
				zap.Stringer("txid", txid),
				zap.Any("phase", state.Phase),
				zap.Any("cluster", cid))
			state.Info.Type = raftpb.Prepared
			state.Info.Clusters = append(make([]raftpb.Cluster, 0), raftpb.Cluster{Id: uint64(cid)})
			if err := m.rn.ProposeMerge(context.Background(), state.Info); err != nil {
				m.lg.Debug("propose to cluster prepare failed", zap.Error(err))
			}

		case raftpb.MsgMergePrepareNo: // on coordinator
			// validation and duplication check
			if state.Phase == committed {
				panic(fmt.Sprintf("tx %v should abort but committed", txid))
			} else if state.Phase != prepareYes {
				m.lg.Debug("duplicate tx message",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))
				break
			}

			m.lg.Debug("ready to abort tx", zap.Stringer("txid", txid))
			state.Info.Type = raftpb.Abort
			if err := m.rn.ProposeMerge(context.Background(), state.Info); err != nil {
				m.lg.Debug("propose to abort tx failed", zap.Error(err))
			}

		case raftpb.MsgMergeAck: // on coordinator
			// validation ack
			if state.Phase == ackedCommit || state.Phase == ackedAbort {
				continue
			}
			if state.Phase != committed && state.Phase != aborted {
				panic(fmt.Sprintf("should be acked but state is %v", state.Phase))
			}

			// find cluster id of sender
			var cid types.ID
			for clr, mems := range state.Clusters {
				for _, member := range mems {
					if member.ID == msg.From {
						cid = clr
					}
				}
			}
			if cid == 0 {
				m.lg.Debug("unknown Cid",
					zap.Stringer("type", msg.Type),
					zap.Uint64("from", msg.From),
					zap.Stringer("txid", txid))
				break
			}

			m.lg.Debug("ready to propose cluster ack",
				zap.Stringer("txid", txid),
				zap.Any("phase", state.Phase),
				zap.Any("cluster", cid))
			state.Info.Type = raftpb.Ack
			state.Info.Clusters = append(make([]raftpb.Cluster, 0), raftpb.Cluster{Id: uint64(cid)})
			if err := m.rn.ProposeMerge(context.Background(), state.Info); err != nil {
				m.lg.Debug("propose to ack tx failed", zap.Error(err))
			}
		}

		m.transport.Send(resps)
	}
}
