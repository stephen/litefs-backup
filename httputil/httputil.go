package httputil

import (
	"context"
	"encoding/json"
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

// RenderResponse renders API response as JSON.
func RenderResponse(w http.ResponseWriter, resp any) error {
	w.Header().Set("Content-Type", "application/json")
	if buf, err := json.MarshalIndent(resp, "", "  "); err != nil {
		return err
	} else if _, err := w.Write(buf); err != nil {
		return err
	}

	return nil
}
