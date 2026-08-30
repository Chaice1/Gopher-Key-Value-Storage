package httptransport

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/Chaice1/Gopher-Key-Value-Storage/internal/cluster"
)

func LeaderCheckMiddleware(next http.Handler, n *cluster.Node) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cluster.Leader != n.GetState() {

			leaderAddr := n.GetLeaderAddress()
			if leaderAddr == "" {
				SendResponse(w, http.StatusServiceUnavailable, "service unavailable")
				return
			}

			leaderAddr = "http://" + leaderAddr

			targetURL, err := url.Parse(leaderAddr)
			if err != nil {
				SendResponse(w, http.StatusInternalServerError, "invalid leader address configuration")
				return
			}

			proxy := httputil.NewSingleHostReverseProxy(targetURL)

			r.Host = targetURL.Host

			proxy.ServeHTTP(w, r)
			return
		}

		next.ServeHTTP(w, r)
	})
}
