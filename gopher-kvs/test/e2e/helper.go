package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	raftpb "github.com/Chaice1/Gopher-Key-Value-Storage/gen"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/storage"
	grpcTransport "github.com/Chaice1/Gopher-Key-Value-Storage/internal/transport/grpc"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/wal"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TestNode struct {
	Node         *cluster.Node
	GrpcServer   *grpc.Server
	Port         int
	Cancel       context.CancelFunc
	LogPath      string
	MetadataPath string
}

type TestCluster struct {
	Nodes  []*TestNode
	Cancel context.CancelFunc
}

func StartCluster(t *testing.T, count int) *TestCluster {
	t.Helper()
	ports := make([]int, count)

	for i := range count {
		lis, err := net.Listen("tcp", "localhost:0")

		if err != nil {
			t.Fatalf("failed  to get free port: %v", err)
		}

		ports[i] = lis.Addr().(*net.TCPAddr).Port
		if err := lis.Close(); err != nil {
			slog.Error("failed to close listener", "error", err)
		}
	}

	nodeIDs := make([]string, count)
	for i := range count {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
	}

	allClients := make([]raftpb.RaftClient, count)

	for i := range count {
		addr := fmt.Sprintf("localhost:%d", ports[i])
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			t.Fatalf("failed to create grpc client for %s: %v", addr, err)
		}
		allClients[i] = raftpb.NewRaftClient(conn)
	}

	ctx, cancelAll := context.WithCancel(context.Background())

	nodes := make([]*TestNode, count)

	for i := range count {
		tmpDir := t.TempDir()

		logPath := fmt.Sprintf("%s/log.wal", tmpDir)
		metaPath := fmt.Sprintf("%s/meta.wal", tmpDir)

		handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})
		log := slog.New(handler).With(slog.String("node_id", nodeIDs[i]))
		slog.SetDefault(log)

		s := storage.NewStorage()
		w, err := wal.NewWal(logPath, metaPath, log, s)
		if err != nil {
			t.Fatalf("failed to create wal for %s: %v", nodeIDs[i], s)
		}

		t.Cleanup(func() {
			if wErr := w.Close(); wErr != nil {
				t.Log("failed to close wal", "error", wErr)
			}
		})

		nodeAddr := fmt.Sprintf("localhost:%d", ports[i]+1000)
		node, err := cluster.NewNode(
			nodeIDs[i],
			uint32(count),
			nodeIDs,
			allClients,
			log,
			w,
			s,
			nodeAddr,
		)
		if err != nil {
			t.Fatalf("failed to create node %s: %v", nodeIDs[i], err)
		}
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", ports[i]))
		if err != nil {
			t.Fatalf("failed to listen on port %d:%v", ports[i], err)
		}

		server := grpc.NewServer()
		grpcHandler := grpcTransport.NewgrpcHandler(node)
		raftpb.RegisterRaftServer(server, grpcHandler)
		go func() {
			if err := server.Serve(lis); err != nil {
				log.Error("failed to create new server", "error", err)
			}
		}()

		nodeCtx, nodeCancel := context.WithCancel(ctx)
		go node.Run(nodeCtx)

		nodes[i] = &TestNode{
			Node:         node,
			GrpcServer:   server,
			Port:         ports[i],
			Cancel:       nodeCancel,
			LogPath:      logPath,
			MetadataPath: metaPath,
		}
	}

	tc := &TestCluster{
		Nodes:  nodes,
		Cancel: cancelAll,
	}

	t.Cleanup(func() {
		tc.Stop()
	})

	return tc
}

func (tc *TestCluster) Stop() {
	tc.Cancel()

	for _, tn := range tc.Nodes {
		tn.GrpcServer.Stop()
	}
}

func (tc *TestCluster) WaitForLeader(t *testing.T, timeout time.Duration) *TestNode {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for leader election")
			return nil
		case <-tick.C:
			for _, tn := range tc.Nodes {
				if tn.Node.GetState() == cluster.Leader {
					return tn
				}
			}

		}
	}
}

func (tc *TestCluster) FindLeader() *TestNode {
	for _, tn := range tc.Nodes {
		if tn.Node.GetState() == cluster.Leader {
			return tn
		}
	}
	return nil
}

func ArrangeLeaderWork(t *testing.T) (*TestCluster, *TestNode) {
	t.Helper()
	tc := StartCluster(t, 3)

	leader := tc.WaitForLeader(t, 10*time.Second)

	assert.NotNil(t, leader)
	return tc, leader
}

func AddCommandReqToLeader(leader *TestNode, c byte, key string, val []byte, respCh chan bool) {
	leader.Node.CommandCh <- &cluster.CommandRequest{
		Command:    c,
		Key:        key,
		Value:      val,
		ResponseCh: respCh,
	}
}
