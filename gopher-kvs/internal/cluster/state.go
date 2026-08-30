package cluster

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

type VoteMessageReq struct {
	NodeID      string
	Term        uint64
	LastLogIdx  int64
	LastLogTerm uint64
	ResponseCh  chan *VoteMessageRes
}

type VoteMessageRes struct {
	Term  uint64
	Agree bool
}

type AppendEntriesRes struct {
	Term    uint64
	Success bool
}
type AppendEntriesReq struct {
	Term            uint64
	LeaderAddress   string
	Key             string
	Val             []byte
	Command         byte
	LastCommitedIdx int64
	LastPrevIdx     int64
	PrevTerm        uint64
	EntryTerm       uint64
	ResponseCh      chan *AppendEntriesRes
}

type CommandRequest struct {
	Command    byte
	Key        string
	Value      []byte
	ResponseCh chan bool
}

type LogEntry struct {
	Term    uint64
	Command byte
	Key     string
	Value   []byte
}
