package slackapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/openclaw/slacrawl/internal/store"
)

func reusableThreadFingerprint(ctx context.Context, st *store.Store, workspace, channel, ts string, fingerprint threadFingerprint, now time.Time, maxAge time.Duration) (bool, error) {
	key := workspace + "|" + channel + "|" + ts
	value, err := st.GetSyncState(ctx, SourceUser, "thread_fingerprint_v2", key)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var previous threadFingerprint
	if json.Unmarshal([]byte(value), &previous) != nil || previous.CheckedAt.IsZero() || previous.ReplyCount != fingerprint.ReplyCount || previous.LatestReply != fingerprint.LatestReply || fingerprint.LatestReply == "" || now.Before(previous.CheckedAt) || (maxAge > 0 && now.Sub(previous.CheckedAt) >= maxAge) {
		return false, nil
	}
	_, err = st.GetSyncState(ctx, SourceUser, "thread_skip", key)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	return st.ThreadRepliesMatch(ctx, workspace, channel, ts, fingerprint.ReplyCount, fingerprint.LatestReply)
}

// Slack leaves old parents at their original timestamp after a first/new reply.
// One paginated metadata traversal covers both recent ingestion and old parents.
// Only recent messages and old roots needing replies enter the write batch.
func incrementalHistoryWrites(ctx context.Context, st *store.Store, workspace, channel string, messages []rawConversationMessage, lower string, now time.Time, known, preexisting map[string]bool) ([]rawConversationMessage, error) {
	bound, err := strconv.ParseFloat(lower, 64)
	if err != nil {
		return nil, err
	}
	result := make([]rawConversationMessage, 0, len(messages))
	for _, raw := range messages {
		m := raw.Message
		ts, err := strconv.ParseFloat(m.Timestamp, 64)
		if err != nil || math.IsNaN(ts) || math.IsInf(ts, 0) {
			return nil, errors.New("invalid message timestamp in incremental parent scan")
		}
		if ts >= bound {
			result = append(result, raw)
			continue
		}
		if m.ThreadTimestamp != "" && m.ThreadTimestamp != m.Timestamp {
			continue
		}
		if m.ReplyCount <= 0 && !known[m.Timestamp] && !preexisting[m.Timestamp] {
			continue
		}
		fingerprint := threadFingerprint{ReplyCount: m.ReplyCount, LatestReply: m.LatestReply}
		if !preexisting[m.Timestamp] {
			same, err := reusableThreadFingerprint(ctx, st, workspace, channel, m.Timestamp, fingerprint, now, 0)
			if err != nil {
				return nil, err
			}
			if same {
				continue
			}
		}
		result = append(result, raw)
	}
	return result, nil
}
