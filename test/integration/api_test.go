//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yousufmo27/taskq"
	"github.com/yousufmo27/taskq/internal/api"
	"github.com/yousufmo27/taskq/internal/testutil"
)

type apiClient struct {
	t     *testing.T
	base  string
	token string
}

func newAPI(t *testing.T, token string) *apiClient {
	t.Helper()
	return newAPIWith(t, api.Options{Token: token})
}

func newAPIWith(t *testing.T, o api.Options) *apiClient {
	t.Helper()
	srv := httptest.NewServer(api.New(testutil.Shared(t), o).Handler())
	t.Cleanup(srv.Close)
	return &apiClient{t: t, base: srv.URL, token: o.Token}
}

func (a *apiClient) do(method, path string, body any) (int, http.Header, map[string]any) {
	a.t.Helper()
	var r io.Reader
	if body != nil {
		switch b := body.(type) {
		case string:
			r = strings.NewReader(b)
		default:
			buf, _ := json.Marshal(b)
			r = bytes.NewReader(buf)
		}
	}
	req, _ := http.NewRequest(method, a.base+path, r)
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			a.t.Fatalf("%s %s: non-JSON body %q", method, path, raw)
		}
	}
	return resp.StatusCode, resp.Header, out
}

func TestAPIEnqueueGetAndDedup(t *testing.T) {
	t.Parallel()
	a := newAPI(t, "")
	q := testutil.Queue(t)

	code, _, body := a.do("POST", "/v1/queues/"+q+"/jobs", map[string]any{
		"type": "email", "payload": map[string]string{"to": "a@example.com"},
		"priority": "high", "idempotency_key": "welcome-1",
	})
	if code != http.StatusAccepted {
		t.Fatalf("enqueue: %d %v", code, body)
	}
	id := body["id"].(string)

	code, _, body = a.do("POST", "/v1/queues/"+q+"/jobs", map[string]any{
		"type": "email", "idempotency_key": "welcome-1",
	})
	if code != http.StatusOK || body["id"] != id || body["duplicate"] != true {
		t.Fatalf("dedup: %d %v", code, body)
	}

	code, _, body = a.do("GET", "/v1/queues/"+q+"/jobs/"+id, nil)
	if code != http.StatusOK || body["state"] != "pending" || body["priority"] != "high" ||
		body["idempotency_key"] != "welcome-1" {
		t.Fatalf("get: %d %v", code, body)
	}
	if p := body["payload"].(map[string]any); p["to"] != "a@example.com" {
		t.Fatalf("payload round trip: %v", body["payload"])
	}

	code, _, body = a.do("GET", "/v1/queues/"+q+"/stats", nil)
	pr := body["priorities"].(map[string]any)["high"].(map[string]any)
	if code != http.StatusOK || pr["waiting"] != float64(1) {
		t.Fatalf("stats: %d %v", code, body)
	}
	code, _, body = a.do("GET", "/v1/queues", nil)
	if code != http.StatusOK || !strings.Contains(strings.Join(toStrings(body["queues"]), ","), q) {
		t.Fatalf("list queues: %d %v", code, body)
	}
}

func toStrings(v any) []string {
	var out []string
	for _, x := range v.([]any) {
		out = append(out, x.(string))
	}
	return out
}

func TestAPIValidationAndErrors(t *testing.T) {
	t.Parallel()
	a := newAPI(t, "")
	q := testutil.Queue(t)
	cases := []struct {
		method, path string
		body         any
		want         int
	}{
		{"POST", "/v1/queues/" + q + "/jobs", map[string]any{}, 400},                              // no type
		{"POST", "/v1/queues/" + q + "/jobs", map[string]any{"type": "x", "priority": "p0"}, 400}, // bad priority
		{"POST", "/v1/queues/" + q + "/jobs", map[string]any{"type": "x", "bogus": 1}, 400},       // unknown field
		{"POST", "/v1/queues/" + q + "/jobs", "{not json", 400},
		{"POST", "/v1/queues/" + q + "/jobs", `{"type":"x"} {"type":"y"}`, 400}, // trailing data
		{"POST", "/v1/queues/bad:name/jobs", map[string]any{"type": "x"}, 400},
		{"POST", "/v1/queues/" + q + "/jobs", `{"type":"x","payload":"` + strings.Repeat("a", api.MaxBodyBytes) + `"}`, 413},
		{"GET", "/v1/queues/" + q + "/jobs/nope", nil, 404},
		{"POST", "/v1/queues/" + q + "/dead/nope/replay", nil, 404},
		{"GET", "/v1/queues/" + q + "/dead?limit=0", nil, 400},
		{"DELETE", "/v1/queues/" + q + "/dead?older_than=yesterday", nil, 400},
		{"PUT", "/v1/queues/" + q + "/config", map[string]any{"rate": -1}, 400},
	}
	for _, c := range cases {
		if code, _, body := a.do(c.method, c.path, c.body); code != c.want {
			t.Errorf("%s %s: %d %v, want %d", c.method, c.path, code, body, c.want)
		}
	}
}

func TestAPIQueueFullReturns429(t *testing.T) {
	t.Parallel()
	a := newAPI(t, "")
	q := testutil.Queue(t)
	if code, _, body := a.do("PUT", "/v1/queues/"+q+"/config", map[string]any{"max_depth": 1}); code != 200 || body["max_depth"] != float64(1) {
		t.Fatalf("config: %d %v", code, body)
	}
	a.do("POST", "/v1/queues/"+q+"/jobs", map[string]any{"type": "x"})
	code, hdr, _ := a.do("POST", "/v1/queues/"+q+"/jobs", map[string]any{"type": "x"})
	if code != http.StatusTooManyRequests || hdr.Get("Retry-After") == "" {
		t.Fatalf("full: %d Retry-After=%q", code, hdr.Get("Retry-After"))
	}
}

func TestAPIDeadLetterInspectReplayPurge(t *testing.T) {
	t.Parallel()
	rdb := testutil.Shared(t)
	a := newAPI(t, "")
	q := testutil.Queue(t)
	c := taskq.NewClient(rdb)

	stop := runWorker(t, newWorker(t, rdb, q, 2, func(context.Context, *taskq.Job) error {
		return taskq.Permanent(errors.New("nope"))
	}))
	var ids []string
	for range 3 {
		_, _, body := a.do("POST", "/v1/queues/"+q+"/jobs", map[string]any{"type": "x"})
		ids = append(ids, body["id"].(string))
	}
	for _, id := range ids {
		waitState(t, c, q, id, taskq.StateDead)
	}
	stop()

	code, _, body := a.do("GET", "/v1/queues/"+q+"/dead?limit=2", nil)
	jobs := body["jobs"].([]any)
	if code != 200 || len(jobs) != 2 || jobs[0].(map[string]any)["last_error"] != "permanent: nope" {
		t.Fatalf("list dead: %d %v", code, body)
	}
	if code, _, _ := a.do("POST", "/v1/queues/"+q+"/dead/"+ids[0]+"/replay", nil); code != http.StatusNoContent {
		t.Fatalf("replay one: %d", code)
	}
	if code, _, _ := a.do("POST", "/v1/queues/"+q+"/dead/"+ids[0]+"/replay", nil); code != http.StatusConflict {
		t.Fatalf("replay a non-dead job: %d, want 409", code)
	}
	if code, _, body := a.do("POST", "/v1/queues/"+q+"/dead/replay?limit=1", nil); code != 200 || body["replayed"] != float64(1) {
		t.Fatalf("replay batch: %d %v", code, body)
	}
	if code, _, body := a.do("DELETE", "/v1/queues/"+q+"/dead?older_than=0s", nil); code != 200 || body["purged"] != float64(1) {
		t.Fatalf("purge: %d %v", code, body)
	}
}

func TestAPIAuthAndHealth(t *testing.T) {
	t.Parallel()
	a := newAPI(t, "s3cret")
	q := testutil.Queue(t)
	if code, _, _ := a.do("GET", "/v1/queues/"+q+"/stats", nil); code != 200 {
		t.Fatalf("with token: %d", code)
	}
	a.token = "wrong"
	if code, _, _ := a.do("GET", "/v1/queues/"+q+"/stats", nil); code != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", code)
	}
	a.token = ""
	if code, _, _ := a.do("GET", "/healthz", nil); code != 200 {
		t.Fatalf("healthz: %d", code)
	}
	if code, _, _ := a.do("GET", "/readyz", nil); code != 200 {
		t.Fatalf("readyz: %d", code)
	}
}
