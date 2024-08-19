package httputil

import (
	"context"
	"log"
	"net/http"
)

// APIHandler transforms a handler func with a returning error into Go's http.Handler
func APIHandler(fn func(ctx context.Context, w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fn(r.Context(), w, r); err != nil {
			// XXX: Error(w, r, err)
			log.Println(err)
		}
	})
}
