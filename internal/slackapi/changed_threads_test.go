package slackapi

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestChangedThreadsFullHistory(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer st.Close()
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	count, replies, histories := 0, 0, 0
	latest := "1710000002.000000"
	failPage := false
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
		parent := map[string]any{"ts": "1710000001.000000", "text": "old parent", "reply_count": count, "latest_reply": latest}
		switch r.URL.Path {
		case "/conversations.history":
			histories++
			require.Empty(t, form.Get("oldest"), "every scheduled scan must visit old parents")
			return map[string]any{"ok": true, "messages": []any{parent}}, nil
		case "/conversations.replies":
			replies++
			require.Equal(t, "1710000001.000000", form.Get("ts"))
			if form.Get("cursor") == "next" {
				return map[string]any{"ok": false, "error": "fixture_failure"}, nil
			}
			messages := []any{parent}
			if count > 1 {
				messages = append(messages, map[string]any{"ts": "1710000002.000000", "thread_ts": "1710000001.000000", "text": "first reply"})
			}
			if count > 2 {
				messages = append(messages, map[string]any{"ts": "1710000003.000000", "thread_ts": "1710000001.000000", "text": "middle reply"})
			}
			messages = append(messages, map[string]any{"ts": latest, "thread_ts": "1710000001.000000", "text": "latest reply"})
			result := map[string]any{"ok": true, "messages": messages}
			if failPage {
				result["response_metadata"] = map[string]any{"next_cursor": "next"}
			}
			return result, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	client.now = func() time.Time { return now }
	opts := SyncOptions{WorkspaceID: "T123", Full: true, ChangedThreads: true}
	run := func() { t.Helper(); require.NoError(t, client.Sync(ctx, st, opts)); now = now.Add(time.Minute) }
	run()
	require.Zero(t, replies)
	count = 1
	run()
	require.Equal(t, 1, replies, "first reply to old zero-reply parent is fetched")
	run()
	require.Equal(t, 1, replies, "unchanged thread must reuse completed fetch")
	count = 2
	latest = "1710000003.000000"
	run()
	require.Equal(t, 2, replies)
	run()
	require.Equal(t, 2, replies)
	latest = "1710000004.000000"
	require.NoError(t, client.Sync(ctx, st, opts))
	require.Equal(t, 3, replies, "latest timestamp change triggers fetch even at same count")
	// Latest change with old archived reply makes count conservative; start a new
	// stable two-reply state to exercise failed-pagination proof invalidation.
	count = 3
	failPage = true
	require.Error(t, client.Sync(ctx, st, opts))
	before := replies
	failPage = false
	run()
	require.Greater(t, replies, before, "failed page must not create a reusable fingerprint")
	// Use the known stable archived shape for a complete fingerprint.
	count = 3
	run()
	now = now.Add(25 * time.Hour)
	before = replies
	run()
	require.Greater(t, replies, before, "periodic reconciliation fetches unchanged threads")
	require.Greater(t, histories, 8)
}

func TestChangedThreadsRequiresFullHistory(t *testing.T) {
	st := mustStore(t)
	defer st.Close()
	client := New(config.Tokens{})
	for _, opts := range []SyncOptions{{ChangedThreads: true}, {ChangedThreads: true, Full: true, Since: "1"}, {ChangedThreads: true, Full: true, LatestOnly: true}} {
		require.ErrorContains(t, client.Sync(context.Background(), st, opts), "changed-threads requires")
	}
}

func TestChangedThreadsConservativeProof(t *testing.T) {
	for _, scenario := range []string{"expired", "pending", "skipped", "root-changed-during-fetch", "incomplete-count"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer st.Close()
			now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
			calls := 0
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				parent := map[string]any{"ts": "1710000001.000000", "reply_count": 1, "latest_reply": "1710000002.000000"}
				switch r.URL.Path {
				case "/conversations.history":
					return map[string]any{"ok": true, "messages": []any{parent}}, nil
				case "/conversations.replies":
					calls++
					if scenario == "root-changed-during-fetch" {
						parent["latest_reply"] = "1710000003.000000"
					}
					messages := []any{parent}
					if scenario != "incomplete-count" {
						messages = append(messages, map[string]any{"ts": "1710000002.000000", "thread_ts": "1710000001.000000"})
					}
					return map[string]any{"ok": true, "messages": messages}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			client.now = func() time.Time { return now }
			opts := SyncOptions{WorkspaceID: "T123", Full: true, ChangedThreads: true}
			firstErr := client.Sync(ctx, st, opts)
			mismatch := scenario == "root-changed-during-fetch" || scenario == "incomplete-count"
			if mismatch {
				require.ErrorContains(t, firstErr, "scan remains incomplete")
			} else {
				require.NoError(t, firstErr)
			}
			require.Equal(t, 1, calls)
			if mismatch {
				incomplete, err := st.HasIncompleteAPIHistory(ctx, "T123")
				require.NoError(t, err)
				require.True(t, incomplete)
				pending, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
				require.NoError(t, err)
				require.NotEmpty(t, pending)
			}
			switch scenario {
			case "expired":
				now = now.Add(25 * time.Hour)
			case "pending":
				_, err := st.ApplyWriteBatch(ctx, store.WriteBatch{PendingThreads: []store.ThreadWork{{SourceName: SourceUser, WorkspaceID: "T123", ChannelID: "C123", TS: "1710000001.000000"}}})
				require.NoError(t, err)
			case "skipped":
				require.NoError(t, st.SetSyncState(ctx, SourceUser, "thread_skip", "T123|C123|1710000001.000000", "missing_scope"))
			}
			secondErr := client.Sync(ctx, st, opts)
			if mismatch {
				require.ErrorContains(t, secondErr, "scan remains incomplete")
			} else {
				require.NoError(t, secondErr)
			}
			require.Equal(t, 2, calls)
		})
	}
}

func TestChangedThreadsDeletedArchivedReply(t *testing.T) {
	for _, markDeleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "historical-extra", true: "deleted-tombstone"}[markDeleted], func(t *testing.T) {
			ctx := context.Background()
			st := mustStore(t)
			defer st.Close()
			count, calls := 2, 0
			client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
				latest := "1710000003.000000"
				if count == 0 {
					latest = ""
				}
				parent := map[string]any{"ts": "1710000001.000000", "reply_count": count, "latest_reply": latest}
				switch r.URL.Path {
				case "/conversations.history":
					return map[string]any{"ok": true, "messages": []any{parent}}, nil
				case "/conversations.replies":
					calls++
					messages := []any{parent}
					if count == 2 {
						messages = append(messages, map[string]any{"ts": "1710000002.000000", "thread_ts": "1710000001.000000"})
					}
					if count > 0 {
						messages = append(messages, map[string]any{"ts": latest, "thread_ts": "1710000001.000000"})
					}
					return map[string]any{"ok": true, "messages": messages}, nil
				}
				return primaryOwnerResponse(r.URL.Path), nil
			})
			opts := SyncOptions{WorkspaceID: "T123", Full: true, ChangedThreads: true}
			run := func() {
				t.Helper()
				require.NoError(t, client.Sync(ctx, st, opts))
				incomplete, err := st.HasIncompleteAPIHistory(ctx, "T123")
				require.NoError(t, err)
				require.False(t, incomplete)
			}
			run()
			require.Equal(t, 1, calls)
			count = 1
			if markDeleted {
				require.NoError(t, st.MarkMessageDeleted(ctx, store.Message{WorkspaceID: "T123", ChannelID: "C123", TS: "1710000002.000000", ThreadTS: "1710000001.000000", DeletedTS: "1710000004.000000", SourceName: SourceUser, SourceRank: 1, UpdatedAt: time.Now()}, nil))
			}
			run()
			require.Equal(t, 2, calls)
			run()
			if markDeleted {
				require.Equal(t, 2, calls, "deleted rows must not count as active replies")
			} else {
				require.Equal(t, 3, calls, "unmarked historical extras conservatively prevent caching")
			}
			count = 0
			run()
			before := calls
			run()
			require.Greater(t, calls, before, "zero live replies remains complete despite historical extras")
		})
	}
}
