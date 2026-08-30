package httptransport

import (
	"net/http"

	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
)

func NewHTTPHandler(n *cluster.Node) (http.Handler, error) {

	mux := http.NewServeMux()
	Addroutes(mux, n)

	return mux, nil
}

func Addroutes(mux *http.ServeMux, n *cluster.Node) {
	mux.Handle("POST /set", LeaderCheckMiddleware(http.HandlerFunc(NewSetHandler(n)), n))
	mux.Handle("GET /get", NewGetHandler(n))
	mux.Handle("DELETE /delete/{key}", LeaderCheckMiddleware(http.HandlerFunc(NewDeleteHandler(n)), n))
}
