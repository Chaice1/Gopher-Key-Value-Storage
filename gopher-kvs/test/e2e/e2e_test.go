package e2e

import (
	"bytes"
	"log/slog"
	"sync"
	"testing"
	"time"

	raftpb "github.com/Chaice1/Gopher-Key-Value-Storage/gen"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/storage"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_LeaderElection(t *testing.T) {
	tc, _ := ArrangeLeaderWork(t)

	leaderCount := 0
	followerCount := 0
	for _, tn := range tc.Nodes {
		if tn.Node.GetState() == cluster.Leader {
			leaderCount++
		} else if tn.Node.GetState() == cluster.Follower {
			followerCount++
		}

	}

	assert.EqualValues(t, 1, leaderCount)
	assert.EqualValues(t, 2, followerCount)

}

func TestE2E_SetAndGet(t *testing.T) {
	tc, leader := ArrangeLeaderWork(t)

	test := struct {
		Command    byte
		Key        string
		Value      []byte
		ResponseCh chan bool
	}{
		Command:    'S',
		Key:        "test",
		Value:      storage.FromStringToBytes("test"),
		ResponseCh: make(chan bool, 1),
	}

	AddCommandReqToLeader(leader, test.Command, test.Key, test.Value, test.ResponseCh)

	var value []byte
	require.Eventually(t, func() bool {
		for _, node := range tc.Nodes {
			if node.Port == leader.Port {
				continue
			}

			if val, err := node.Node.GetValFromMap(test.Key); err == nil {
				value = val
				return true
			}
		}

		return false
	}, 5*time.Second, 500*time.Millisecond)

	assert.NotNil(t, value)
	assert.EqualValues(t, test.Value, value)
}

func TestE2E_DeleteKey(t *testing.T) {
	tc, leader := ArrangeLeaderWork(t)

	test := struct {
		Key        string
		Value      []byte
		ResponseCh chan bool
	}{
		Key:        "test",
		Value:      []byte("test"),
		ResponseCh: make(chan bool, 1),
	}

	AddCommandReqToLeader(leader, 'S', test.Key, test.Value, test.ResponseCh)
	res := <-test.ResponseCh
	require.True(t, res, "Set command failed on leader")

	AddCommandReqToLeader(leader, 'D', test.Key, nil, test.ResponseCh)
	res = <-test.ResponseCh
	require.True(t, res, "Delete command failed on leader")

	require.Eventually(t, func() bool {
		for _, node := range tc.Nodes {
			_, err := node.Node.GetValFromMap(test.Key)
			if err == nil {
				return false
			}
			if err != storage.ErrorNotFound {
				return false
			}
		}
		return true

	}, 5*time.Second, 100*time.Millisecond, "key was not deleted from all nodes")
}

func TestE2E_LeaderStepDown(t *testing.T) {

	_, leader := ArrangeLeaderWork(t)

	test := struct {
		Term       uint64
		ResponseCh chan *cluster.AppendEntriesRes
		PrevIdx    int64
	}{
		Term:       leader.Node.GetTerm() + 1,
		ResponseCh: make(chan *cluster.AppendEntriesRes, 1),
		PrevIdx:    -1,
	}

	leader.Node.LeaderCh <- &cluster.AppendEntriesReq{
		Term:        test.Term,
		ResponseCh:  test.ResponseCh,
		LastPrevIdx: test.PrevIdx,
	}

	res := <-test.ResponseCh

	state := leader.Node.GetState()
	assert.EqualValues(t, state, cluster.Follower)
	assert.EqualValues(t, test.Term, res.Term)

}

func TestE2E_WALRecovery(t *testing.T) {
	tc, leader := ArrangeLeaderWork(t)

	test := struct {
		Command    byte
		Key        string
		Value      []byte
		ResponseCh chan bool
	}{
		Command:    'S',
		Key:        "test",
		Value:      storage.FromStringToBytes("test"),
		ResponseCh: make(chan bool, 1),
	}
	AddCommandReqToLeader(leader, 'S', test.Key, test.Value, test.ResponseCh)
	<-test.ResponseCh

	time.Sleep(5 * time.Second)

	leader.Cancel()
	leader.GrpcServer.Stop()
	if err := leader.Node.Wal.Close(); err != nil {
		t.Log("failed to close wal", "error", err)
	}

	newStorage := storage.NewStorage()
	newWal, err := wal.NewWal(leader.LogPath, leader.MetadataPath, slog.Default(), newStorage)

	require.NoError(t, err)
	t.Cleanup(func() {
		if err := newWal.Close(); err != nil {
			t.Log("failed to close wal", "error", err)
		}
	})

	newNode, err := cluster.NewNode(
		"node",
		uint32(len(tc.Nodes)),
		[]string{"node1", "node2", "node3"},
		make([]raftpb.RaftClient, len(tc.Nodes)),
		slog.Default(),
		newWal,
		newStorage,
		leader.Node.GetLeaderAddress(),
	)

	require.NoError(t, err)

	assert.Equal(t, leader.Node.GetTerm(), newNode.GetTerm())

	recoveredVal, err := newNode.GetValFromMap(test.Key)
	require.NoError(t, err)
	assert.Equal(t, test.Value, recoveredVal)

}

func TestE2E_ConcurrentSets(t *testing.T) {
	tc, leader := ArrangeLeaderWork(t)

	numNotes := 5

	testTable := []struct {
		Key        string
		Value      []byte
		ResponseCh chan bool
	}{
		{
			Key:        "123",
			Value:      []byte("123"),
			ResponseCh: make(chan bool, 1),
		},
		{
			Key:        "202020",
			Value:      []byte("12000"),
			ResponseCh: make(chan bool, 1),
		},
		{
			Key:        "key",
			Value:      []byte("99999"),
			ResponseCh: make(chan bool, 1),
		},
		{
			Key:        "ans",
			Value:      []byte("4444"),
			ResponseCh: make(chan bool, 1),
		},
		{
			Key:        "test",
			Value:      []byte("test"),
			ResponseCh: make(chan bool, 1),
		},
	}
	wg := &sync.WaitGroup{}

	wg.Add(numNotes)

	for i := range testTable {
		go func() {
			defer wg.Done()
			AddCommandReqToLeader(leader, 'S', testTable[i].Key, testTable[i].Value, testTable[i].ResponseCh)
			res := <-testTable[i].ResponseCh
			assert.True(t, res)
		}()
	}

	wg.Wait()

	require.Eventually(t, func() bool {
		for _, node := range tc.Nodes {
			for _, test := range testTable {
				val, err := node.Node.GetValFromMap(test.Key)

				if err != nil {
					return false
				}

				if !bytes.Equal(val, test.Value) {
					return false
				}
			}

		}
		return true
	}, 5*time.Second, 500*time.Millisecond)

}
