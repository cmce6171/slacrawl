package slackapi

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/openclaw/slacrawl/internal/config"
	"github.com/openclaw/slacrawl/internal/store"
	"github.com/stretchr/testify/require"
)

func TestIncrementalThreadsFindsFirstAndLaterRepliesToOldParents(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer st.Close()
	now := time.Unix(1793000000, 0).UTC()
	count, calls := 0, 0
	failAudit := false
	var bounds []string
	old := "1710000001.000000"
	other := "1710000002.000000"
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
		parent := map[string]any{"ts": old, "text": "parent", "reply_count": count}
		if count > 0 {
			parent["latest_reply"] = strconv.FormatInt(1793000000+int64(count), 10) + ".000000"
		}
		switch r.URL.Path {
		case "/conversations.history":
			bounds = append(bounds, form.Get("oldest"))
			oldest, _ := strconv.ParseFloat(form.Get("oldest"), 64)
			if oldest > 1710000002 {
				return map[string]any{"ok": true, "messages": []any{}}, nil
			}
			result := map[string]any{"ok": true, "messages": []any{parent}}
			if form.Get("cursor") == "" {
				return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": other, "text": "unchanged history"}}, "response_metadata": map[string]any{"next_cursor": "old-parent-page"}}, nil
			}
			if failAudit {
				result["has_more"] = true
			}
			return result, nil
		case "/conversations.replies":
			calls++
			messages := []any{parent}
			for i := 1; i <= count; i++ {
				messages = append(messages, map[string]any{"ts": strconv.FormatInt(1793000000+int64(i), 10) + ".000000", "thread_ts": old, "text": "reply"})
			}
			return map[string]any{"ok": true, "messages": messages}, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	client.now = func() time.Time { return now }
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: true, ChangedThreads: true}))
	before, err := st.QueryReadOnly(ctx, "select updated_at from messages where channel_id='C123' and ts='1710000002.000000'")
	require.NoError(t, err)
	require.Len(t, before, 1)
	opts := SyncOptions{WorkspaceID: "T123", IncrementalThreads: true}
	now = now.Add(time.Minute)
	count = 1
	bounds = nil
	require.NoError(t, client.Sync(ctx, st, opts))
	require.Equal(t, 1, calls, "first reply to a years-old zero-reply parent must be fetched")
	require.Len(t, bounds, 2)
	require.Empty(t, bounds[0], "old-parent metadata must remain exhaustive")
	now = now.Add(time.Minute)
	require.NoError(t, client.Sync(ctx, st, opts))
	require.Equal(t, 1, calls, "unchanged old replies reuse successful proof")
	now = now.Add(time.Minute)
	count = 2
	require.NoError(t, client.Sync(ctx, st, opts))
	require.Equal(t, 2, calls)
	after, err := st.QueryReadOnly(ctx, "select updated_at from messages where channel_id='C123' and ts='1710000002.000000'")
	require.NoError(t, err)
	require.Equal(t, before, after, "old unchanged rows must not be rewritten")
	require.NoError(t, st.MarkMessageDeleted(ctx, store.Message{WorkspaceID: "T123", ChannelID: "C123", TS: "1793000001.000000", ThreadTS: old, DeletedTS: "1793000030.000000", SourceName: SourceUser, SourceRank: 1, UpdatedAt: now}, nil))
	now = now.Add(time.Minute)
	require.NoError(t, client.Sync(ctx, st, opts))
	require.Equal(t, 3, calls, "matching parent metadata must not hide missing live local replies")
	now = now.Add(25 * time.Hour)
	require.NoError(t, client.Sync(ctx, st, opts))
	require.Equal(t, 3, calls, "unchanged threads do not expire in the fast path; reconciliation runs separately")
	now = now.Add(time.Minute)
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: true}))
	require.Equal(t, 4, calls, "separate full reconciliation rereads unchanged replies")
	now = now.Add(time.Minute)
	require.NoError(t, client.Sync(ctx, st, opts))
	require.Equal(t, 4, calls, "full reconciliation refreshes proof for subsequent polls")
	failAudit = true
	now = now.Add(time.Minute)
	require.ErrorContains(t, client.Sync(ctx, st, opts), "cursor")
	h, err := st.APIHistory(ctx, store.APIHistoryScope{SourceName: SourceUser, WorkspaceID: "T123", ChannelID: "C123"})
	require.NoError(t, err)
	require.NotNil(t, h.Pending)
	failAudit = false
	now = now.Add(time.Minute)
	require.NoError(t, client.Sync(ctx, st, opts))
	jobs, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestIncrementalThreadsFirstReplyAtRetentionFloor(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer st.Close()
	now := time.Unix(1793000000, 0).UTC()
	count, calls := 0, 0
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
		parent := map[string]any{"ts": "1710000001.000000", "reply_count": count}
		if count > 0 {
			parent["latest_reply"] = "1793000001.000000"
		}
		switch r.URL.Path {
		case "/conversations.history":
			messages := []any{parent}
			if form.Get("oldest") == "1710000001.000000" && form.Get("inclusive") != "1" {
				messages = []any{}
			}
			return map[string]any{"ok": true, "messages": messages}, nil
		case "/conversations.replies":
			calls++
			return map[string]any{"ok": true, "messages": []any{parent, map[string]any{"ts": "1793000001.000000", "thread_ts": "1710000001.000000"}}}, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	client.now = func() time.Time { return now }
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: true}))
	require.NoError(t, st.SetSyncState(ctx, "retention", "channel_floor", "T123|C123", "1710000001.000000"))
	now = now.Add(time.Minute)
	count = 1
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", IncrementalThreads: true}))
	require.Equal(t, 1, calls, "the retained floor is inclusive for first-reply detection")
}

func TestIncrementalThreadsRetriesDurableWorkAfterReplyHintLoss(t *testing.T) {
	ctx := context.Background()
	st := mustStore(t)
	defer st.Close()
	now := time.Unix(1793000000, 0).UTC()
	stage := 0
	calls := 0
	generation := ""
	client := primaryOwnerClient(t, config.Tokens{User: "fixture-user"}, func(r *http.Request, form url.Values) (any, error) {
		count := 1
		if stage > 0 {
			count = 2
		}
		parent := map[string]any{"ts": "1710000001.000000", "text": "root", "reply_count": count, "latest_reply": strconv.FormatInt(1793000000+int64(count), 10) + ".000000"}
		switch r.URL.Path {
		case "/conversations.history":
			if stage == 2 {
				return map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "1710000001.000000", "text": "root"}}}, nil
			}
			return map[string]any{"ok": true, "messages": []any{parent}}, nil
		case "/conversations.replies":
			calls++
			if stage == 1 {
				return map[string]any{"ok": false, "error": "fixture_failure"}, nil
			}
			if stage == 2 {
				jobs, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
				require.NoError(t, err)
				require.Len(t, jobs, 1)
				require.Equal(t, generation, jobs[0].Generation)
			}
			messages := []any{parent}
			for i := 1; i <= count; i++ {
				messages = append(messages, map[string]any{"ts": strconv.FormatInt(1793000000+int64(i), 10) + ".000000", "thread_ts": "1710000001.000000", "text": "reply"})
			}
			return map[string]any{"ok": true, "messages": messages}, nil
		}
		return primaryOwnerResponse(r.URL.Path), nil
	})
	client.now = func() time.Time { return now }
	require.NoError(t, client.Sync(ctx, st, SyncOptions{WorkspaceID: "T123", Full: true}))
	stage = 1
	now = now.Add(25 * time.Hour)
	opts := SyncOptions{WorkspaceID: "T123", IncrementalThreads: true}
	require.Error(t, client.Sync(ctx, st, opts))
	jobs, err := st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	generation = jobs[0].Generation
	stage = 2
	now = now.Add(time.Minute)
	require.NoError(t, client.Sync(ctx, st, opts))
	jobs, err = st.PendingThreadWork(ctx, SourceUser, "T123", "C123")
	require.NoError(t, err)
	require.Empty(t, jobs)
	require.Equal(t, 3, calls)
}
