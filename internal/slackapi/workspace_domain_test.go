package slackapi

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openclaw/slacrawl/internal/admission"
	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
)

func TestSyncRecordsWorkspaceDomainFromAuthURL(t *testing.T) {
	for _, tc := range []struct {
		name, authURL, existing, want string
	}{
		{"from auth URL", "https://fixture.slack.com/", "", "fixture"},
		{"auth URL replaces stale domain", "https://fixture.slack.com/", "old", "fixture"},
		{"missing URL keeps known domain", "", "fixture", "fixture"},
		{"non-Slack URL keeps known domain", "https://example.com/", "fixture", "fixture"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := admissionServer(t, []map[string]any{}, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/auth.test" {
					return false
				}
				writeAdmissionJSON(t, w, map[string]any{"ok": true, "team_id": "T123", "team": "Fixture", "url": tc.authURL})
				return true
			})
			defer server.Close()
			st := mustStore(t)
			defer st.Close()
			ctx := context.Background()
			if tc.existing != "" {
				require.NoError(t, st.UpsertWorkspace(ctx, store.Workspace{ID: "T123", Name: "Fixture", Domain: tc.existing, RawJSON: "{}", UpdatedAt: time.Unix(0, 0)}))
			}
			client := NewWithOptions(config.Tokens{Bot: "fixture"}, server.URL()+"/", server.Client()).WithDMPolicy(admission.Exclude)
			require.NoError(t, client.Sync(ctx, st, SyncOptions{}))
			rows, err := st.QueryReadOnly(ctx, "select domain from workspaces where id = 'T123'")
			require.NoError(t, err)
			require.Equal(t, []map[string]any{{"domain": tc.want}}, rows)
		})
	}
}
