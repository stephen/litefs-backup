package httputil

import (
	"net/http"
	"strings"

	lfsb "github.com/stephen/litefs-backup"
)

// CheckClusterNoAuth pulls the cluster information out of the request, doing no
// auth{nz} checks.
func CheckClusterNoAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := strings.Split(r.Header.Get("Authorization"), " ")
		if len(h) != 2 || strings.ToLower(h[0]) != "cluster" {
			Error(w, r, lfsb.Errorf(lfsb.ErrorTypeAuth, "EINVALIDAUTH", "authorization header invalid format"))
			return
		}

		cluster := h[1]

		if err := lfsb.ValidateClusterName(cluster); err != nil {
			Error(w, r, err)
			return
		}

		ctx := ContextWithClusterName(r.Context(), cluster)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
