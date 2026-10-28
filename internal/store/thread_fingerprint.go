package store

import "context"

// ThreadRepliesMatch prevents cached fetch proof from surviving retention,
// missing archive rows, or a response inconsistent with parent metadata.
func (s *Store) ThreadRepliesMatch(ctx context.Context, workspaceID, channelID, root string, count int, latest string) (bool, error) {
	var actual int
	var last string
	var parentMatches bool
	err := s.db.QueryRowContext(ctx, `select count(*), coalesce(max(ts), ''), exists (
 select 1 from messages p where p.workspace_id=? and p.channel_id=? and p.ts=?
 and p.reply_count=? and coalesce(p.latest_reply, '')=?
 and coalesce(p.deleted_ts, '')='' and coalesce(p.subtype, '')<>'message_deleted'
) from messages
 where workspace_id=? and channel_id=? and thread_ts=? and ts<>? and coalesce(deleted_ts, '')=''`, workspaceID, channelID, root, count, latest, workspaceID, channelID, root, root).Scan(&actual, &last, &parentMatches)
	return err == nil && parentMatches && actual == count && last == latest, err
}
