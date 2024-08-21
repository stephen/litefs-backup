package httputil

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/superfly/ltx"
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

var typeToCode = map[lfsb.ErrorType]int{
	lfsb.ErrorTypeConflict:      http.StatusConflict,
	lfsb.ErrorTypeValidation:    http.StatusBadRequest,
	lfsb.ErrorTypeNotFound:      http.StatusNotFound,
	lfsb.ErrorTypeUnprocessable: http.StatusUnprocessableEntity,
}
var codeToType map[int]lfsb.ErrorType

func init() {
	codeToType = make(map[int]lfsb.ErrorType, len(typeToCode))
	for k, v := range typeToCode {
		codeToType[v] = k
	}
}

type ErrorResponse struct {
	Code  string   `json:"code"`
	Error string   `json:"error"`
	Pos   *ltx.Pos `json:"pos,omitempty"`
}

func HTTPCodeToErrorType(code int) lfsb.ErrorType {
	if typ, ok := codeToType[code]; ok {
		return typ
	}
	return lfsb.ErrorTypeUnknown
}
