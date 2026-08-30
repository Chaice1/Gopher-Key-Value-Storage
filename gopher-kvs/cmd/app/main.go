package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/Chaice1/Gopher-Key-Value-Storage/config"
	raftpb "github.com/Chaice1/Gopher-Key-Value-Storage/gen"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/storage"
	grpcTransport "github.com/Chaice1/Gopher-Key-Value-Storage/internal/transport/grpc"
	httpTransport "github.com/Chaice1/Gopher-Key-Value-Storage/internal/transport/http"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/wal"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func runApp() {
	cfg := config.MustLoad()

	Peers := make([]config.Peer, 0)
	if cfg.Peers != "" {
		peersList := strings.Split(cfg.Peers, ",")
		for _, p := range peersList {
			parts := strings.Split(p, "=")
			if len(parts) == 2 {
				Peers = append(Peers, config.Peer{
					ID:   parts[0],
					Addr: parts[1],
				})
			}
		}

	} else {
		slog.Error("failed to read peers config")
		return
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	log := slog.New(handler).With(slog.String("node_id", cfg.NodeID))
	slog.SetDefault(log)

	go func() {
		pprofPort := 6000 + (cfg.GRPCPort % 1000)
		pprofAddr := fmt.Sprintf(":%d", pprofPort)
		slog.Info("starting pprof", slog.String("addr", pprofAddr))

		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(pprofAddr, nil); err != nil {
			slog.Error("pprof failed", "err", err)
		}
	}()

	log.Info("starting GopherKVS", slog.String("node_id", cfg.NodeID))

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	nodeClients, nodeIDs := CreatePeers(Peers)

	s := storage.NewStorage()

	logPath := fmt.Sprintf("/data/log/%s.wal", cfg.NodeID)
	metaDataPath := fmt.Sprintf("/data/metadata/%s.wal", cfg.NodeID)
	wal, err := wal.NewWal(logPath, metaDataPath, log, s)
	if err != nil {
		slog.Error("failed to create wal", "error", err)
		return
	}

	defer wal.Close()

	node, err := cluster.NewNode(cfg.NodeID, uint32(len(Peers))+1, nodeIDs, nodeClients, log, wal, s, cfg.NodeAddress)
	if err != nil {
		slog.Error("failed to create node")
		return
	}

	go node.Run(ctx)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		slog.Error("failed to listen port", "GRPCport", cfg.GRPCPort)
		return
	}

	grpcServer := grpc.NewServer()
	grpcHandler := grpcTransport.NewgrpcHandler(node)
	raftpb.RegisterRaftServer(grpcServer, grpcHandler)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server failed ", "err", err)
		}
	}()

	httpHandler, err := httpTransport.NewHTTPHandler(node)
	if err != nil {
		slog.Error("failed to listen port", "HTTPport", cfg.HTTPPort)
		grpcServer.Stop()
		return
	}

	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: httpHandler,
	}

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server failed", "err", err)
		}
	}()

	slog.Info("node is running", "id", cfg.NodeID, "GRPCport", cfg.GRPCPort)

	<-ctx.Done()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown failed", "err", err)
	} else {
		slog.Info("HTTP server stopped")
	}

	slog.Info("shutting down gracefully...")
	grpcServer.GracefulStop()
	slog.Info("stopped")

}

func CreatePeers(peers []config.Peer) ([]raftpb.RaftClient, []string) {
	var clients []raftpb.RaftClient
	var ids []string

	for _, p := range peers {
		conn, err := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			slog.Error("failed to connect to peer", "id", p.ID, "address", p.Addr)
			continue
		}

		clients = append(clients, raftpb.NewRaftClient(conn))
		ids = append(ids, p.ID)
	}
	return clients, ids
}

func main() {
	runApp()
}
