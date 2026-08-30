package httptransport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
)

func SendResponse(w http.ResponseWriter, status int, message string) {
	w.Header().Add("Content-type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{"message": message}); err != nil {
		slog.Error("failed to write message", "error", err)
	}
}

func AddTaskToCommandCh(ctx context.Context, w http.ResponseWriter, key string, val []byte, op byte, responseCh chan bool, n *cluster.Node) bool {
	select {
	case <-ctx.Done():
		SendResponse(w, http.StatusBadRequest, "context is canceled")
		return false
	case n.CommandCh <- &cluster.CommandRequest{
		Command:    op,
		Key:        key,
		Value:      val,
		ResponseCh: responseCh,
	}:
		return true
	}
}

func NewDeleteHandler(n *cluster.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		responseCh := make(chan bool, 1)

		key := r.PathValue("key")

		if !AddTaskToCommandCh(r.Context(), w, key, nil, 'D', responseCh, n) {
			return
		}
		select {
		case <-r.Context().Done():
			SendResponse(w, http.StatusBadRequest, "context is canceled")
			return
		case <-time.After(5 * time.Second):
			SendResponse(w, http.StatusServiceUnavailable, "timeout is expired")
			return
		case success := <-responseCh:
			if success {
				SendResponse(w, http.StatusOK, "key is deleted successfully")
				return
			} else {
				SendResponse(w, http.StatusInternalServerError, "internal server error")
				return
			}
		}
	}
}

func NewSetHandler(n *cluster.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		reqBody := &SetKeyRequest{}
		if err := json.NewDecoder(r.Body).Decode(reqBody); err != nil {
			SendResponse(w, http.StatusBadRequest, "wrong format request")
			return
		}

		responseCh := make(chan bool, 1)

		if !AddTaskToCommandCh(r.Context(), w, reqBody.Key, []byte(reqBody.Val), 'S', responseCh, n) {
			return
		}
		select {
		case <-r.Context().Done():
			SendResponse(w, http.StatusBadRequest, "context is canceled")
			return
		case <-time.After(5 * time.Second):
			SendResponse(w, http.StatusGatewayTimeout, "timeout is expired")
			return
		case success := <-responseCh:
			if success {
				SendResponse(w, http.StatusOK, "key is set successfully")
				return
			} else {
				SendResponse(w, http.StatusInternalServerError, "internal server error")
				return
			}

		}

	}
}

func NewGetHandler(n *cluster.Node) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		val, err := n.GetValFromMap(key)
		if err != nil {
			SendResponse(w, http.StatusNotFound, "key is not found")
			return
		}
		SendResponse(w, http.StatusOK, fmt.Sprintf("key is fetched successfully, value of key: %s", val))

	}
}
