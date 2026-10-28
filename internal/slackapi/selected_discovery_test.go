package slackapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/stretchr/testify/require"
)

func TestSelectedDiscoveryAvoidsWorkspaceCatalogAndPreservesDMs(t *testing.T) {
	var mu sync.Mutex
	var infos, histories []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T123","team":"fixture","user_id":"U123"}`))
		case "/conversations.info":
			require.Equal(t, "fixture-user", r.Form.Get("token"))
			id := r.Form.Get("channel")
			mu.Lock()
			infos = append(infos, id)
			mu.Unlock()
			row := map[string]any{"id": id, "name": "selected"}
			switch id {
			case "D123":
				row["is_im"] = true
				row["user"] = "U123"
			case "C234":
				row["is_mpim"] = true
				row["members"] = []string{"U123"}
			default:
				row["is_channel"] = true
			}
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": row}))
		case "/conversations.history":
			mu.Lock()
			histories = append(histories, r.Form.Get("channel"))
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true,"messages":[],"response_metadata":{"next_cursor":""}}`))
		case "/users.list":
			_, _ = w.Write([]byte(`{"ok":true,"members":[],"response_metadata":{"next_cursor":""}}`))
		case "/conversations.list", "/conversations.join":
			t.Errorf("scoped sync must not call %s", r.URL.Path)
			http.Error(w, "unexpected catalog or join", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewWithOptions(config.Tokens{User: "fixture-user"}, server.URL+"/", server.Client())
	client.sleep = func(context.Context, time.Duration) error { return nil }
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	require.NoError(t, client.Sync(context.Background(), st, SyncOptions{
		WorkspaceID: "T123", Channels: []string{"C123", "D123", "C234", "C123", "C999"},
		ExcludeChannels: []string{"C999"}, SelectedDiscovery: true, Full: true, ChangedThreads: true, AutoJoin: testBoolPtr(false),
	}))
	require.ElementsMatch(t, []string{"C123", "D123", "C234"}, infos)
	require.ElementsMatch(t, []string{"C123", "D123", "C234"}, histories)
	status, err := st.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, status.Channels)
}

func TestSelectedDiscoveryRejectsMissingAllowlistBeforeNetwork(t *testing.T) {
	client := NewWithOptions(config.Tokens{User: "fixture-user"}, "http://127.0.0.1:1/", nil)
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	require.ErrorContains(t, client.Sync(context.Background(), st, SyncOptions{SelectedDiscovery: true}), "requires explicit channel IDs")
}

func TestSelectedDiscoveryRejectsMismatchedMetadataBeforeHistory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth.test":
			_, _ = w.Write([]byte(`{"ok":true,"team_id":"T123","team":"fixture","user_id":"U123"}`))
		case "/conversations.info":
			_, _ = w.Write([]byte(`{"ok":true,"channel":{"id":"COTHER","is_channel":true}}`))
		default:
			t.Errorf("metadata mismatch must stop before %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewWithOptions(config.Tokens{User: "fixture-user"}, server.URL+"/", server.Client())
	st := mustStore(t)
	defer func() { require.NoError(t, st.Close()) }()
	require.ErrorContains(t, client.Sync(context.Background(), st, SyncOptions{SelectedDiscovery: true, Channels: []string{"C123"}}), "metadata identity mismatch")
}
