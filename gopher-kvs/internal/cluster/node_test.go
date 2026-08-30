package cluster

import (
	"log/slog"
	"os"
	"sync"
	"testing"

	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/metrics"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockWal struct{}

func (m *mockWal) Write(int64, uint64, byte, string, []byte) error {
	return nil
}

func (m *mockWal) RecoverLogHistory() ([]LogEntry, error) {
	return nil, nil
}
func (m *mockWal) RecoverMetaData() (uint64, string, int64, error) {
	return 0, "", 0, nil
}
func (m *mockWal) WriteMetaData(uint64, string, int64) error {
	return nil
}
func (m *mockWal) Close() error {
	return nil
}

func TestCluster_handleAppendEntries(t *testing.T) {

	testTable := []struct {
		Name               string
		Term               uint64
		Key                string
		Val                []byte
		Command            byte
		reqLastCommitedIdx int64
		reqPrevTerm        uint64
		reqLastPrevIdx     int64
		reqEntryTerm       uint64
		expectedAgree      bool
		expectedTerm       uint64
	}{
		{
			Name:          "Term лидера меньше чем у ноды",
			Term:          4,
			expectedAgree: false,
			expectedTerm:  5,
		},
		{
			Name:           "Индекс предыдущей записи у лидера больше чем индекс последней записи у ноды",
			Term:           6,
			reqLastPrevIdx: 4,
			expectedAgree:  false,
			expectedTerm:   6,
		},
		{
			Name:           "Тёрмы логов не совпадают у лидера и ноды предыдущие",
			Term:           6,
			reqLastPrevIdx: 1,
			reqPrevTerm:    2,
			expectedAgree:  false,
			expectedTerm:   6,
		},
		{
			Name:               "Успех",
			Term:               6,
			reqLastPrevIdx:     1,
			reqPrevTerm:        3,
			expectedAgree:      true,
			Key:                "test",
			Val:                []byte("test"),
			reqEntryTerm:       6,
			Command:            'S',
			reqLastCommitedIdx: 1,
			expectedTerm:       6,
		},
	}

	for _, tt := range testTable {
		t.Run(tt.Name, func(t *testing.T) {

			node := Node{
				nodeID: "test_node",
				term:   5,
				state:  Follower,
				mu:     &sync.RWMutex{},
				Wal:    &mockWal{},
				logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)),
				log: []LogEntry{
					{Term: 1, Command: 'S', Key: "test1", Value: []byte("test1")},
					{Term: 3, Command: 'S', Key: "test2", Value: []byte("test2")},
				},
				lastLogIdx:      1,
				lastCommitedIdx: 1,
				metrics:         *metrics.NewMetrics("test_node"),
				s:               storage.NewStorage(),
			}

			req := &AppendEntriesReq{
				Term:            tt.Term,
				Key:             tt.Key,
				Val:             tt.Val,
				Command:         tt.Command,
				LastCommitedIdx: tt.reqLastCommitedIdx,
				LastPrevIdx:     tt.reqLastPrevIdx,
				PrevTerm:        tt.reqPrevTerm,
				EntryTerm:       tt.reqEntryTerm,
				ResponseCh:      make(chan *AppendEntriesRes, 1),
			}

			handleAppendEntries(&node, req)
			res := <-req.ResponseCh

			require.Equal(t, tt.expectedAgree, res.Success)

			assert.Equal(t, tt.expectedTerm, res.Term, "Неверный терм в ответе лидеру")
			assert.Equal(t, tt.expectedTerm, node.term, "Неверный внутренний терм ноды")

			if tt.expectedAgree {
				require.Equal(t, req.LastPrevIdx+1, node.lastLogIdx, "Лог не вырос")

				lastEntry := node.log[node.lastLogIdx]
				assert.Equal(t, req.EntryTerm, lastEntry.Term)
				assert.Equal(t, req.Key, lastEntry.Key)
			}
		})
	}
}
func TestCluster_handleRequestVote(t *testing.T) {

	testTable := []struct {
		Name           string
		reqTerm        uint64
		reqLastLogIdx  int64
		reqLastLogTerm uint64
		expectedAgree  bool
	}{
		{
			Name:           "Term кандидата меньше нашего",
			reqTerm:        4,
			reqLastLogIdx:  5,
			reqLastLogTerm: 4,
			expectedAgree:  false,
		},
		{
			Name:           "Лог кандидата старее нашего",
			reqTerm:        6,
			reqLastLogIdx:  5,
			reqLastLogTerm: 3,
			expectedAgree:  false,
		},
		{
			Name:           "Term логов равны но наш лог длиннее",
			reqTerm:        6,
			reqLastLogIdx:  0,
			reqLastLogTerm: 4,
			expectedAgree:  false,
		},
		{
			Name:           "Успех: отдаём голос хорошему кандидата",
			reqTerm:        6,
			reqLastLogIdx:  2,
			reqLastLogTerm: 4,
			expectedAgree:  true,
		},
	}

	for _, tt := range testTable {
		t.Run(tt.Name, func(t *testing.T) {
			node := Node{
				nodeID: "test_node",
				term:   5,
				state:  Follower,
				mu:     &sync.RWMutex{},
				Wal:    &mockWal{},
				logger: &slog.Logger{},
				log: []LogEntry{
					{Term: 2, Command: 'S', Key: "test1", Value: []byte("test1")},
					{Term: 4, Command: 'S', Key: "test2", Value: []byte("test2")},
				},
				lastLogIdx: 1,
				metrics:    *metrics.NewMetrics("test_node"),
			}

			req := &VoteMessageReq{
				Term:        tt.reqTerm,
				NodeID:      "test_candidate",
				LastLogIdx:  tt.reqLastLogIdx,
				LastLogTerm: tt.reqLastLogTerm,
				ResponseCh:  make(chan *VoteMessageRes, 1),
			}

			handleRequestVote(&node, req)

			res := <-req.ResponseCh

			assert.Equal(t, tt.expectedAgree, res.Agree)

			if tt.expectedAgree {
				assert.Equal(t, tt.reqTerm, node.term)
				assert.Equal(t, "test_candidate", node.votedFor)
			}
		})
	}
}
