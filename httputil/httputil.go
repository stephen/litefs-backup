package httputil

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/superfly/ltx"
)

// APIHandler transforms a handler func with a returning error into Go's http.Handler
func APIHandler(fn func(ctx context.Context, w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fn(r.Context(), w, r); err != nil {
			Error(w, r, err)
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
	lfsb.ErrorTypeAuth:          http.StatusUnauthorized,
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

func Error(w http.ResponseWriter, r *http.Request, err error) {
	var status int
	var resp ErrorResponse

	var pmErr *ltx.PosMismatchError
	if errors.As(err, &pmErr) {
		status = http.StatusConflict
		resp = ErrorResponse{
			Code:  "EPOSMISMATCH",
			Error: "position mismatch",
			Pos:   &pmErr.Pos,
		}

		slog.Debug("position mismatch",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Any("error", pmErr))
	} else {
		var lfscErr *lfsb.Error
		if errors.As(err, &lfscErr) {
			status = ErrorTypeToHTTPCode(lfscErr.Type)
			resp = ErrorResponse{
				Code:  lfscErr.Code,
				Error: lfscErr.Message,
			}
		} else {
			status = http.StatusInternalServerError
			resp = ErrorResponse{
				Code:  "EINTERNAL",
				Error: err.Error(),
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

// Err returns an LFSC error from an error response.
func (r *ErrorResponse) Err(statusCode int) error {
	switch r.Code {
	case "EPOSMISMATCH":
		var pos ltx.Pos
		if r.Pos != nil {
			pos = *r.Pos
		}
		return ltx.NewPosMismatchError(pos)

	default:
		return lfsb.Errorf(HTTPCodeToErrorType(statusCode), r.Code, r.Error)
	}
}

func ErrorTypeToHTTPCode(typ lfsb.ErrorType) int {
	if code, ok := typeToCode[typ]; ok {
		return code
	}
	return http.StatusInternalServerError
}
