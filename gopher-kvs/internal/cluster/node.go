package cluster

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	raftpb "github.com/Chaice1/Gopher-Key-Value-Storage/gen"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Wal interface {
	Write(int64, uint64, byte, string, []byte) error
	RecoverLogHistory() ([]LogEntry, error)
	RecoverMetaData() (uint64, string, int64, error)
	WriteMetaData(uint64, string, int64) error
	Close() error
}

type Storage interface {
	Get(string) ([]byte, error)
	Set(string, []byte)
	Delete(string) error
}

type Node struct {
	Wal              Wal
	nodeID           string
	nodeAddress      string
	leaderAddress    string
	term             uint64
	votes            uint32
	state            State
	countNodes       uint32
	votedFor         string
	LeaderCh         chan *AppendEntriesReq
	VoteMessageReqCh chan *VoteMessageReq
	CommandCh        chan *CommandRequest
	UpdateCh         chan string
	peers            map[string]raftpb.RaftClient
	mu               *sync.RWMutex
	logger           *slog.Logger
	s                Storage
	lastLogIdx       int64
	lastCommitedIdx  int64
	log              []LogEntry
	mNextIdxsPeers   map[string]int64
	metrics          metrics.Metrics
}

func (n *Node) GetTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

func (n *Node) GetLeaderAddress() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderAddress
}

func (n *Node) GetState() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func NewNode(nodeID string, countNodes uint32, nodeIDs []string, nodeClients []raftpb.RaftClient, logger *slog.Logger, w Wal, s Storage, nodeAddress string) (*Node, error) {
	peers := make(map[string]raftpb.RaftClient, len(nodeIDs)-1)
	for i := range nodeIDs {
		if nodeID == nodeIDs[i] {
			continue
		}
		peers[nodeIDs[i]] = nodeClients[i]
	}
	log, err := w.RecoverLogHistory()

	if err != nil {
		return nil, err
	}
	var lastLogIdx int64
	lastLogIdx = -1
	if len(log) != 0 {
		lastLogIdx = int64(len(log)) - 1
	}

	term, votedFor, lastCommitedIdx, err := w.RecoverMetaData()
	if err != nil {
		return nil, err
	}

	for i := range lastCommitedIdx + 1 {
		switch log[i].Command {
		case 'S':
			s.Set(log[i].Key, log[i].Value)
		case 'D':
			if err := s.Delete(log[i].Key); err != nil {
				return nil, err
			}
		}
	}

	node := &Node{
		Wal:              w,
		nodeID:           nodeID,
		nodeAddress:      nodeAddress,
		term:             term,
		votes:            0,
		state:            Follower,
		countNodes:       countNodes,
		LeaderCh:         make(chan *AppendEntriesReq, 1),
		VoteMessageReqCh: make(chan *VoteMessageReq, 1),
		CommandCh:        make(chan *CommandRequest, 1),
		mu:               &sync.RWMutex{},
		peers:            peers,
		logger:           logger,
		s:                s,
		log:              log,
		lastLogIdx:       lastLogIdx,
		lastCommitedIdx:  lastCommitedIdx,
		mNextIdxsPeers:   make(map[string]int64, len(nodeIDs)),
		UpdateCh:         make(chan string, 1),
		votedFor:         votedFor,
		metrics:          *metrics.NewMetrics(nodeID),
	}
	node.metrics.State.Set(0)
	return node, nil
}

func (n *Node) SetValToMapForTest(key string, val []byte) {
	n.s.Set(key, val)
}

func (n *Node) GetValFromMap(key string) ([]byte, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.s.Get(key)
}
func (n *Node) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			switch n.GetState() {
			case Leader:
				n.runLeader(ctx)
			case Follower:
				n.runFollower(ctx)
			case Candidate:
				n.runCandidate(ctx)
			}
		}

	}
}
func HandleCallPeers(ctx context.Context, n *Node) {

	for nodeID, peer := range n.peers {
		go func() {
			var logEntry LogEntry
			var isCommandSent = false
			n.mu.RLock()
			val := n.mNextIdxsPeers[nodeID]
			if len(n.log) > 0 && val >= 0 && val <= n.lastLogIdx {
				logEntry = n.log[val]
				isCommandSent = true
			}

			var PrevTerm uint64

			if val-1 >= 0 && val-1 <= n.lastLogIdx {
				PrevTerm = n.log[val-1].Term
			}
			currTerm := n.term
			lastCommitedIdx := n.lastCommitedIdx
			leaderAddress := n.nodeAddress
			n.mu.RUnlock()

			reqCtx, reqCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer reqCancel()
			res, err := peer.AppendEntries(reqCtx, &raftpb.AppendEntriesRequest{
				Term:          currTerm,
				LeaderAddress: leaderAddress,
				Key:           logEntry.Key,
				Val:           logEntry.Value,
				Command:       uint32(logEntry.Command),
				LastPrevIdx:   val - 1,
				LastCommitIdx: lastCommitedIdx,
				PrevTerm:      PrevTerm,
				EntryTerm:     logEntry.Term,
			})
			if err != nil {
				return
			}
			n.mu.Lock()
			if res.Term > n.term {

				n.state = Follower
				n.term = res.Term
				n.votedFor = ""
				if err := n.Wal.WriteMetaData(n.term, n.votedFor, n.lastCommitedIdx); err != nil {
					n.logger.Error("failed to write metadata", "error", err)
				}
				n.metrics.State.Set(0)
				n.metrics.Term.Set(float64(n.term))
				reqCancel()
				n.mu.Unlock()
				return
			}
			needUpdate := false
			if res.Success {
				if isCommandSent {
					n.mNextIdxsPeers[nodeID] = val + 1
				}
				needUpdate = true
			} else if val > 0 {
				n.mNextIdxsPeers[nodeID]--
			}
			n.mu.Unlock()

			if needUpdate {
				select {
				case n.UpdateCh <- nodeID:
				default:
				}
			}
		}()
	}

}
func (n *Node) runLeader(ctx context.Context) {
	n.logger.Info("Leader is ready")
	tick := time.NewTicker(time.Duration(500) * time.Millisecond)

	n.mu.Lock()
	for nodeID := range n.peers {
		n.mNextIdxsPeers[nodeID] = n.lastLogIdx + 1
	}
	n.mu.Unlock()
	defer tick.Stop()

	pendingRequests := make(map[uint64]chan bool)
	defer func() {
		for _, ch := range pendingRequests {
			select {
			case ch <- false:
			default:
			}
		}
	}()
	HandleCallPeers(ctx, n)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			HandleCallPeers(ctx, n)
			if n.GetState() == Follower {
				return
			}

		case msg := <-n.CommandCh:
			if n.GetState() != Leader {
				n.metrics.State.Set(0)
				msg.ResponseCh <- false
				return
			}
			n.mu.Lock()
			if err := n.Wal.Write(n.lastLogIdx+1, n.term, msg.Command, msg.Key, msg.Value); err != nil {
				msg.ResponseCh <- false
				n.mu.Unlock()
				continue
			}

			n.log = append(n.log, LogEntry{
				Term:    n.term,
				Command: msg.Command,
				Key:     msg.Key,
				Value:   msg.Value,
			})

			n.lastLogIdx++

			pendingRequests[uint64(n.lastLogIdx)] = msg.ResponseCh
			n.mu.Unlock()
			HandleCallPeers(ctx, n)

		case <-n.UpdateCh:
			n.mu.Lock()
			for i := n.lastCommitedIdx + 1; i <= n.lastLogIdx; i++ {
				count := uint32(1)
				for _, idx := range n.mNextIdxsPeers {
					if idx >= i {
						count++
					}
				}
				if count >= n.countNodes/2+1 && n.log[i].Term == n.term {

					for j := n.lastCommitedIdx + 1; j <= i; j++ {
						n.metrics.CommitedCommands.Inc()
						LogEntry := n.log[j]

						if LogEntry.Command == 'S' {
							n.s.Set(LogEntry.Key, LogEntry.Value)
						}
						if LogEntry.Command == 'D' {
							if err := n.s.Delete(LogEntry.Key); err != nil {
								n.logger.Error("failed to delete key", "error", err)
							}

						}

						n.lastCommitedIdx++
						if err := n.Wal.WriteMetaData(n.term, n.votedFor, n.lastCommitedIdx); err != nil {
							n.logger.Error("failed to write metadata", "error", err)
						}

						if ch, ok := pendingRequests[uint64(j)]; ok {
							ch <- true
							delete(pendingRequests, uint64(j))
						}

					}

				}
			}
			n.mu.Unlock()

		case msg := <-n.VoteMessageReqCh:
			if !handleRequestVote(n, msg) {
				return
			}

		case msg := <-n.LeaderCh:
			if handleAppendEntries(n, msg) {
				if n.GetState() != Leader {
					return
				}

			}
		}

	}

}

func (n *Node) runFollower(ctx context.Context) {
	n.logger.Info("Follower is ready")
	duration := time.Duration(rand.Int64N(600))*time.Millisecond + 3*time.Second
	timer := time.NewTimer(duration)
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		duration = time.Duration(rand.Int64N(600))*time.Millisecond + 3*time.Second
		timer.Reset(duration)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			n.mu.Lock()
			n.metrics.State.Set(1)
			n.state = Candidate
			n.mu.Unlock()
			return
		case msg := <-n.LeaderCh:
			n.logger.Info("new ping from leader", "leaderAddress", msg.LeaderAddress, "term", msg.Term)
			if handleAppendEntries(n, msg) {
				resetTimer()
			}

		case msg := <-n.VoteMessageReqCh:
			if !handleRequestVote(n, msg) {
				resetTimer()
			}

		}

	}
}

func handleRequestVote(node *Node, msg *VoteMessageReq) bool {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.term > msg.Term {
		msg.ResponseCh <- &VoteMessageRes{
			Term:  node.term,
			Agree: false,
		}
		return true

	}

	if node.term < msg.Term {
		node.term = msg.Term
		node.votedFor = ""
		node.votes = 0
		node.metrics.State.Set(0)
		node.metrics.Term.Set(float64(node.term))
		node.state = Follower
	}

	isUpToDate := false

	var lastLogTerm uint64

	if len(node.log) > 0 {
		lastLogTerm = node.log[node.lastLogIdx].Term
	}

	if lastLogTerm < msg.LastLogTerm {
		isUpToDate = true
	} else if lastLogTerm == msg.LastLogTerm && node.lastLogIdx <= msg.LastLogIdx {
		isUpToDate = true
	}

	if (node.votedFor == "" || node.votedFor == msg.NodeID) && isUpToDate {

		node.votedFor = msg.NodeID
		if err := node.Wal.WriteMetaData(node.term, node.votedFor, node.lastCommitedIdx); err != nil {
			node.logger.Error("failed to write metadata", "error", err)
		}

		msg.ResponseCh <- &VoteMessageRes{
			Term:  node.term,
			Agree: true,
		}

		return false
	}
	msg.ResponseCh <- &VoteMessageRes{
		Term:  node.term,
		Agree: false,
	}

	return true
}
func handleAppendEntries(node *Node, msg *AppendEntriesReq) bool {
	node.mu.Lock()
	defer node.mu.Unlock()
	if node.term > msg.Term {
		msg.ResponseCh <- &AppendEntriesRes{
			Term:    node.term,
			Success: false,
		}
		return false
	}

	node.leaderAddress = msg.LeaderAddress
	node.term = msg.Term
	node.votedFor = ""
	node.votes = 0
	node.state = Follower
	node.metrics.State.Set(0)
	node.metrics.Term.Set(float64(node.term))

	if err := node.Wal.WriteMetaData(node.term, node.votedFor, node.lastCommitedIdx); err != nil {
		node.logger.Error("failed to write metadata", "error", err)
	}
	if msg.LastPrevIdx > node.lastLogIdx {
		msg.ResponseCh <- &AppendEntriesRes{
			Term:    msg.Term,
			Success: false,
		}

		return true

	}

	if msg.LastPrevIdx >= 0 {
		if msg.PrevTerm != node.log[msg.LastPrevIdx].Term {
			node.log = node.log[:msg.LastPrevIdx]
			node.lastLogIdx = int64(len(node.log) - 1)
			msg.ResponseCh <- &AppendEntriesRes{
				Term:    msg.Term,
				Success: false,
			}

			return true
		}

	}

	if msg.Command != 0 {

		next := msg.LastPrevIdx + 1

		if next <= node.lastLogIdx {
			if msg.EntryTerm != node.log[next].Term {
				node.log = node.log[:next]
				node.lastLogIdx = int64(len(node.log) - 1)
			}
		}

		if next > node.lastLogIdx {
			if err := node.Wal.Write(next, msg.EntryTerm, msg.Command, msg.Key, msg.Val); err != nil {
				msg.ResponseCh <- &AppendEntriesRes{
					Term:    msg.Term,
					Success: false,
				}
				return true
			}

			node.log = append(node.log, LogEntry{
				Term:    msg.EntryTerm,
				Command: msg.Command,
				Key:     msg.Key,
				Value:   msg.Val,
			})
			node.lastLogIdx++
		}

	}
	for msg.LastCommitedIdx > node.lastCommitedIdx {

		next := node.lastCommitedIdx + 1
		if next >= int64(len(node.log)) {
			break
		}
		LogEntry := node.log[next]

		if LogEntry.Command == 'S' {
			node.s.Set(LogEntry.Key, LogEntry.Value)
		}
		if LogEntry.Command == 'D' {
			if err := node.s.Delete(LogEntry.Key); err != nil {
				node.logger.Error("failed to delete key", "error", err)
			}
		}

		node.lastCommitedIdx++
		if err := node.Wal.WriteMetaData(node.term, node.votedFor, node.lastCommitedIdx); err != nil {
			node.logger.Error("failed to write metadata", "error", err)
		}
	}

	msg.ResponseCh <- &AppendEntriesRes{
		Term:    node.term,
		Success: true,
	}

	return true
}

func (n *Node) runCandidate(ctx context.Context) {

	duration := 2*time.Second + time.Duration(rand.IntN(500))*time.Millisecond
	ticker := time.After(duration)
	n.mu.Lock()
	n.votes = 1
	n.votedFor = n.nodeID
	n.term++
	n.metrics.Term.Set(float64(n.term))
	if err := n.Wal.WriteMetaData(n.term, n.votedFor, n.lastCommitedIdx); err != nil {
		n.logger.Error("failed to write metadata", "error", err)
	}
	var lastLogTerm uint64
	if len(n.log) > 0 {
		lastLogTerm = n.log[n.lastLogIdx].Term
	}
	lastLogIdx := n.lastLogIdx

	n.mu.Unlock()
	resultCh := make(chan bool, len(n.peers))
	cttx, cancel := context.WithCancel(ctx)
	defer cancel()
	n.metrics.TotalElections.Inc()
	for _, peer := range n.peers {
		go func() {
			resp, err := peer.RequestVote(cttx, &raftpb.VoteRequest{
				Term:        n.term,
				CandidateId: n.nodeID,
				LastLogIdx:  lastLogIdx,
				LastLogTerm: lastLogTerm,
			})

			if err != nil {
				if status.Code(err) == codes.Canceled || err == context.Canceled || err == context.DeadlineExceeded {
					resultCh <- false
					return
				}
				n.logger.Error("failed to request vote", "error", err)
				resultCh <- false
				return
			}

			n.mu.Lock()
			if resp.Term > n.term {
				n.term = resp.Term
				n.state = Follower
				n.votedFor = ""
				n.votes = 0
				n.metrics.Term.Set(float64(n.term))
				n.metrics.State.Set(0)
				if err := n.Wal.WriteMetaData(n.term, n.votedFor, n.lastCommitedIdx); err != nil {
					n.logger.Error("failed to write metadata", "error", err)
				}
				cancel()
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			resultCh <- resp.GetAgree()

		}()
	}

	n.logger.Info("Candidate is ready")
	for {
		select {
		case <-ctx.Done():
			return
		case Agree := <-resultCh:
			if Agree {
				n.mu.Lock()
				n.votes++
				if n.votes >= n.countNodes/2+1 {
					n.metrics.State.Set(2)
					n.state = Leader
					n.leaderAddress = n.nodeAddress
					n.votedFor = ""
					n.votes = 0
					n.mu.Unlock()
					return
				}
				n.mu.Unlock()
			}
		case msg := <-n.LeaderCh:

			handleAppendEntries(n, msg)
			if n.GetState() == Follower {
				return
			}

		case msg := <-n.VoteMessageReqCh:
			handleRequestVote(n, msg)
			if n.GetState() == Follower {
				return
			}
		case <-ticker:
			return
		}
	}
}
