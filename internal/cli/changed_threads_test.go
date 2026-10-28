package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type changedThreadTransport struct{ replies int }

func (f *changedThreadTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	parent := map[string]any{"ts": "1710000001.000000", "reply_count": 1, "latest_reply": "1710000002.000000", "text": "parent"}
	response := map[string]any{"ok": true}
	switch r.URL.Path {
	case "/auth.test":
		response["team_id"] = "T123"
	case "/conversations.list":
		response["channels"] = []any{map[string]any{"id": "C123", "name": "fixture", "is_channel": true}}
	case "/users.list":
		response["members"] = []any{}
	case "/conversations.history":
		response["messages"] = []any{parent}
	case "/conversations.replies":
		f.replies++
		response["messages"] = []any{parent, map[string]any{"ts": "1710000002.000000", "thread_ts": "1710000001.000000", "text": "reply"}}
	default:
		response = map[string]any{"ok": false, "error": "unexpected_fixture_method"}
	}
	data, err := json.Marshal(response)
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(data)), Request: r}, err
}

func TestChangedThreadsCLI(t *testing.T) {
	cfg, path := userPrimaryConfig(t)
	cfg.WorkspaceID = "T123"
	cfg.Sync.IncludeDMs = new(false)
	t.Setenv(cfg.Slack.User.TokenEnv, "fixture-user")
	require.NoError(t, cfg.Save(path))
	fixture := &changedThreadTransport{}
	var stdout, stderr bytes.Buffer
	app := &App{Stdout: &stdout, Stderr: &stderr, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: fixture}}
	args := []string{"--config", path, "--json", "sync", "--source", "api", "--full", "--changed-threads", "--auto-join=false", "--with-media=false"}
	require.NoError(t, app.Run(context.Background(), args))
	require.Equal(t, 1, fixture.replies)
	require.NoError(t, app.Run(context.Background(), args))
	require.Equal(t, 1, fixture.replies, "CLI flag must propagate to native API optimization")
	for _, flags := range [][]string{{"--changed-threads"}, {"--changed-threads", "--full", "--since", "1710000000"}, {"--changed-threads", "--full", "--source", "desktop"}} {
		require.ErrorContains(t, app.Run(context.Background(), append([]string{"--config", path, "sync"}, flags...)), "changed-threads requires")
	}
}

func TestIncrementalThreadsCLI(t *testing.T) {
	cfg, path := userPrimaryConfig(t)
	cfg.WorkspaceID = "T123"
	cfg.Sync.IncludeDMs = new(false)
	t.Setenv(cfg.Slack.User.TokenEnv, "fixture-user")
	require.NoError(t, cfg.Save(path))
	fixture := &changedThreadTransport{}
	var stdout, stderr bytes.Buffer
	app := &App{Stdout: &stdout, Stderr: &stderr, apiURL: "https://fixture.invalid/", httpClient: &http.Client{Transport: fixture}}
	args := []string{"--config", path, "--json", "sync", "--source", "api", "--incremental-threads", "--auto-join=false", "--with-media=false"}
	require.NoError(t, app.Run(context.Background(), args))
	require.Equal(t, 1, fixture.replies)
	require.NoError(t, app.Run(context.Background(), args))
	require.Equal(t, 1, fixture.replies)
	for _, flags := range [][]string{{"--full"}, {"--since", "1710000000"}, {"--latest-only"}, {"--changed-threads"}, {"--source", "desktop"}} {
		require.ErrorContains(t, app.Run(context.Background(), append(args, flags...)), "incremental-threads requires")
	}
}
