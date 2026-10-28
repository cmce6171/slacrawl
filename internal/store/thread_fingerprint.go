package store

import "context"

// ThreadRepliesMatch prevents cached fetch proof from surviving retention,
// missing archive rows, or a response inconsistent with parent metadata.
func (s *Store) ThreadRepliesMatch(ctx context.Context, workspaceID, channelID, root string, count int, latest string) (bool, error) {
	var actual int
	var last string
	err := s.db.QueryRowContext(ctx, `select count(*), coalesce(max(ts), '') from messages
 where workspace_id=? and channel_id=? and thread_ts=? and ts<>? and coalesce(deleted_ts, '')=''`, workspaceID, channelID, root, root).Scan(&actual, &last)
	return err == nil && actual == count && last == latest, err
}
