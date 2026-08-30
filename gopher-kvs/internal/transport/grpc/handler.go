package grpctransport

import (
	"context"

	raftpb "github.com/Chaice1/Gopher-Key-Value-Storage/gen"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
)

type GrpcHandler struct {
	raftpb.UnimplementedRaftServer
	Node *cluster.Node
}

func NewgrpcHandler(node *cluster.Node) *GrpcHandler {
	return &GrpcHandler{
		Node: node,
	}
}
func (grpch *GrpcHandler) RequestVote(ctx context.Context, req *raftpb.VoteRequest) (*raftpb.VoteResponse, error) {

	responseCh := make(chan *cluster.VoteMessageRes, 1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case grpch.Node.VoteMessageReqCh <- &cluster.VoteMessageReq{
		NodeID:      req.GetCandidateId(),
		Term:        req.GetTerm(),
		LastLogIdx:  req.GetLastLogIdx(),
		LastLogTerm: req.GetLastLogTerm(),
		ResponseCh:  responseCh,
	}:
	}

	select {
	case res := <-responseCh:
		return &raftpb.VoteResponse{
			Agree: res.Agree,
			Term:  res.Term,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}

func (grpch *GrpcHandler) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {

	responseCh := make(chan *cluster.AppendEntriesRes, 1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case grpch.Node.LeaderCh <- &cluster.AppendEntriesReq{
		Term:            req.GetTerm(),
		LeaderAddress:   req.GetLeaderAddress(),
		Key:             req.GetKey(),
		Val:             req.GetVal(),
		Command:         byte(req.GetCommand()),
		LastCommitedIdx: req.GetLastCommitIdx(),
		LastPrevIdx:     req.GetLastPrevIdx(),
		PrevTerm:        req.GetPrevTerm(),
		EntryTerm:       req.EntryTerm,
		ResponseCh:      responseCh,
	}:
	}

	select {
	case response := <-responseCh:
		return &raftpb.AppendEntriesResponse{
			Term:    response.Term,
			Success: response.Success,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}
