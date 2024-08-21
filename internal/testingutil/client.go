package testingutil

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	lfsb "github.com/stephen/litefs-backup"
	"github.com/stephen/litefs-backup/httputil"
	"github.com/superfly/ltx"
)

// Client implements the LiteFS Cloud HTTP API client.
type Client struct {
	baseURL url.URL

	AuthToken  string
	Cluster    string
	ClusterID  string
	HTTPClient *http.Client
}

// NewClient returns a new instance of the client.
func NewClient(u url.URL) (*Client, error) {
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid LiteFS Cloud URL scheme: %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("LiteFS Cloud URL host required")
	}

	return &Client{
		baseURL: url.URL{
			Scheme: u.Scheme,
			Host:   u.Host,
		},

		HTTPClient: http.DefaultClient,
	}, nil
}

// PosMap returns the replication position for all databases on the backup service.
func (c *Client) PosMap(ctx context.Context) (map[string]ltx.Pos, error) {
	req, err := c.newRequest(http.MethodGet, "/pos", nil, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	m := make(map[string]ltx.Pos)
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// WriteTx writes an LTX file to the backup service. The file must be
// contiguous with the latest LTX file on the backup service or else it
// will return an ltx.PosMismatchError.
func (c *Client) WriteTx(ctx context.Context, name string, r io.Reader) error {
	req, err := c.newRequest(http.MethodPost, "/db/tx", url.Values{
		"db": {name},
	}, r)
	if err != nil {
		return err
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// newRequest returns a new HTTP request with the given context & auth parameters.
func (c *Client) newRequest(method, path string, q url.Values, body io.Reader) (*http.Request, error) {
	qq := url.Values{}
	if c.Cluster != "" {
		qq.Set("cluster", c.Cluster)
	}
	for k := range q {
		qq.Set(k, q.Get(k))
	}

	u := c.baseURL
	u.Path = path
	u.RawQuery = qq.Encode()

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}

	// Set the auth header if token is provided. Otherwise send without auth.
	if c.AuthToken != "" {
		req.Header.Set("Authorization", c.AuthToken)
	}
	if c.ClusterID != "" {
		req.Header.Set("Litefs-Cluster-Id", c.ClusterID)
	}
	return req, nil
}

// doRequest executes the request and returns an error if the response is not a 2XX.
func (c *Client) doRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	// If this is not a 2XX code then read the body as an error message.
	if !isSuccessfulStatusCode(resp.StatusCode) {
		return nil, readResponseError(resp)
	}

	return resp, nil
}

// readResponseError reads the response body as an error message & closes the body.
func readResponseError(resp *http.Response) error {
	defer func() { _ = resp.Body.Close() }()

	// Read up to 64KB of data from the body for the error message.
	buf, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return err
	}

	// Attempt to decode as a JSON error.
	var e httputil.ErrorResponse
	if err := json.Unmarshal(buf, &e); err != nil {
		return fmt.Errorf("client error (%d): %s", resp.StatusCode, string(buf))
	}

	// Match specific types of errors.
	switch e.Code {
	case "EPOSMISMATCH":
		if e.Pos != nil {
			return ltx.NewPosMismatchError(*e.Pos)
		} else {
			return ltx.NewPosMismatchError(ltx.Pos{})
		}
	default:
		return lfsb.Errorf(httputil.HTTPCodeToErrorType(resp.StatusCode), e.Code, e.Error)
	}
}

func isSuccessfulStatusCode(code int) bool {
	return code >= 200 && code < 300
}
