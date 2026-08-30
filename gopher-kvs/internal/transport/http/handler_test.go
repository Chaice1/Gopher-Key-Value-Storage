package httptransport

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	raftpb "github.com/Chaice1/Gopher-Key-Value-Storage/gen"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockWal struct{}

func (m *mockWal) Write(int64, uint64, byte, string, []byte) error {
	return nil
}

func (m *mockWal) RecoverLogHistory() ([]cluster.LogEntry, error) {
	return nil, nil
}

//nolint:gocritic
func (m *mockWal) RecoverMetaData() (uint64, string, int64, error) {
	return 0, "", -1, nil
}
func (m *mockWal) WriteMetaData(uint64, string, int64) error {
	return nil
}
func (m *mockWal) Close() error {
	return nil
}

func CreateTestNode(t *testing.T) *cluster.Node {
	s := storage.NewStorage()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	node, err := cluster.NewNode(
		"test_node",
		1,
		[]string{"test_node"},
		[]raftpb.RaftClient{},
		logger,
		&mockWal{},
		s,
		"localhost:8080",
	)
	require.NoError(t, err)
	return node
}

func TestHTTP_SetHandler(t *testing.T) {
	node := CreateTestNode(t)

	go func() {
		req := <-node.CommandCh
		req.ResponseCh <- true
	}()

	reqBody := bytes.NewBufferString(`{"key":"user_1", "val":"Ivan"}`)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/set", reqBody)
	w := httptest.NewRecorder()

	handler := NewSetHandler(node)
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

}

func TestHTTP_SetHandler_BadRequest(t *testing.T) {
	node := CreateTestNode(t)

	reqBody := bytes.NewBufferString(`{"key":"user_1", "val":"Ivan"`)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/set", reqBody)
	w := httptest.NewRecorder()

	handler := NewSetHandler(node)
	handler.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHTTP_GetHandler(t *testing.T) {
	node := CreateTestNode(t)

	node.SetValToMapForTest("target", []byte("bingo"))

	getReq := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/get?key=target", nil)
	w := httptest.NewRecorder()

	handler := NewGetHandler(node)
	handler.ServeHTTP(w, getReq)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "bingo")
}

func TestHTTP_DeleteHandler(t *testing.T) {
	node := CreateTestNode(t)

	go func() {
		req := <-node.CommandCh
		req.ResponseCh <- true
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("DELETE /delete/{key}", NewDeleteHandler(node))
	req := httptest.NewRequestWithContext(context.Background(), http.MethodDelete, "/delete/mykey", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}
