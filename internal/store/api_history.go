package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/openclaw/slacrawl/internal/store/storedb"
)

const APIHistoryEntityType = "history_coverage_v1"

// One resolution per historical scope, bound to the exact unfinished checkpoint.
// Keeping it separate preserves compatibility with canonical checkpoint readers.
const APIHistoryResolutionEntityType = "history_resolution_v1"

type apiHistoryResolution struct {
	CheckpointSHA256    string `json:"checkpoint_sha256"`
	CheckpointUpdatedAt string `json:"checkpoint_updated_at"`
	Oldest              string `json:"oldest"`
	Latest              string `json:"latest"`
}

var ErrAPIHistorySuperseded = errors.New("API history attempt was superseded; retry the selected scope")

// Since is the exact normalized invocation bound, separate from ordinary/Full.
type APIHistoryScope struct {
	SourceName  string
	WorkspaceID string
	ChannelID   string
	Since       string
}

// Latest is the completed requested horizon, including empty history.
// Unlike MCP history, it is not a returned-message watermark.
type APIHistoryState struct {
	Complete      bool    `json:"complete"`
	Latest        string  `json:"latest"`
	Pending       *string `json:"pending,omitempty"`
	Generation    string  `json:"generation,omitempty"`
	PendingLatest string  `json:"pending_latest,omitempty"`
}

type APIHistoryOptions struct {
	Full             bool
	RestoreRequested bool
}

type APIHistoryAttempt struct {
	APIHistoryScope
	Generation       string
	Oldest           string
	Latest           string
	EnforceRetention bool
	Inclusive        bool
}

func (scope APIHistoryScope) key() (string, error) {
	if (scope.SourceName != "api-bot" && scope.SourceName != "api-user") ||
		strings.TrimSpace(scope.WorkspaceID) == "" || strings.TrimSpace(scope.ChannelID) == "" {
		return "", errors.New("API history requires a source, workspace and channel")
	}
	if scope.Since != "" {
		if _, err := apiHistoryTimestamp(scope.Since); err != nil {
			return "", err
		}
	}
	key, err := json.Marshal([3]string{scope.WorkspaceID, scope.ChannelID, scope.Since})
	return string(key), err
}

// APIHistory reads only the selected canonical key; it does not audit aliases
// or establish archive-wide coverage.
func (s *Store) APIHistory(ctx context.Context, scope APIHistoryScope) (APIHistoryState, error) {
	key, err := scope.key()
	if err != nil {
		return APIHistoryState{}, err
	}
	return loadAPIHistory(ctx, s.q, scope.SourceName, key)
}

// BeginAPIHistory owns bound selection and acquisition in one writer snapshot.
// An ordinary retry never inherits a previous Full attempt's restore permission.
func (s *Store) BeginAPIHistory(ctx context.Context, scope APIHistoryScope, opts APIHistoryOptions, horizon string) (APIHistoryAttempt, error) {
	if err := ctx.Err(); err != nil {
		return APIHistoryAttempt{}, err
	}
	key, err := scope.key()
	if err != nil {
		return APIHistoryAttempt{}, err
	}
	if _, err := apiHistoryTimestamp(horizon); err != nil {
		return APIHistoryAttempt{}, err
	}
	q, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return APIHistoryAttempt{}, err
	}
	defer rollback()
	if err := apiHistoryOwner(ctx, q, scope); err != nil {
		return APIHistoryAttempt{}, err
	}
	queries := storedb.New(q)
	state, err := loadAPIHistory(ctx, queries, scope.SourceName, key)
	if err != nil {
		return APIHistoryAttempt{}, err
	}
	oldest := ""
	switch {
	case scope.Since != "":
		oldest = scope.Since
	case opts.Full:
	case state.Pending != nil:
		oldest = *state.Pending
	case state.Complete:
		latest, _ := apiHistoryTimestamp(state.Latest)
		oldest = strconv.FormatFloat(math.Max(latest-time.Hour.Seconds(), 0), 'f', 6, 64)
	}
	floor, err := retentionFloor(ctx, q, scope.WorkspaceID, scope.ChannelID)
	if err != nil {
		return APIHistoryAttempt{}, err
	}
	if floor != "" {
		if _, err := apiHistoryTimestamp(floor); err != nil {
			return APIHistoryAttempt{}, err
		}
	}
	restore := opts.RestoreRequested && (opts.Full || scope.Since != "")
	if scope.Since == "" && !opts.Full {
		oldest = (ChannelSyncCursor{RetentionFloor: floor}).ApplyRetentionFloor(oldest)
	}
	enforce := ShouldEnforceRetention(oldest, floor, restore)
	// A delayed caller clock must still request the whole inherited upper bound.
	// Completion will consume this recorded bound, never its caller's clock.
	for _, prior := range []string{state.Latest, state.PendingLatest} {
		if prior != "" {
			value, _ := apiHistoryTimestamp(prior)
			current, _ := apiHistoryTimestamp(horizon)
			if value > current {
				horizon = prior
			}
		}
	}
	state.Pending = new(oldest)
	state.Generation = rand.Text()
	state.PendingLatest = horizon
	if err := saveAPIHistory(ctx, queries, scope.SourceName, key, state); err != nil {
		return APIHistoryAttempt{}, err
	}
	if err := ctx.Err(); err != nil {
		return APIHistoryAttempt{}, err
	}
	if err := commit(); err != nil {
		return APIHistoryAttempt{}, err
	}
	return APIHistoryAttempt{APIHistoryScope: scope, Generation: state.Generation, Oldest: oldest, Latest: horizon,
		EnforceRetention: enforce, Inclusive: enforce && floor != "" && oldest == floor}, nil
}

func (s *Store) CheckAPIHistory(ctx context.Context, attempt APIHistoryAttempt) error {
	_, err := checkAPIHistory(ctx, s.db, &attempt)
	return err
}

// CompleteAPIHistory cannot replace a newer Pending or complete a revoked scan.
func (s *Store) CompleteAPIHistory(ctx context.Context, attempt APIHistoryAttempt) error {
	q, commit, rollback, err := s.beginMessageTransaction(ctx, true)
	if err != nil {
		return err
	}
	defer rollback()
	state, err := checkAPIHistory(ctx, q, &attempt)
	if err != nil {
		return err
	}
	attempt.Oldest, attempt.Latest = *state.Pending, state.PendingLatest
	state.Latest = state.PendingLatest
	state.Complete = true
	state.Pending, state.Generation, state.PendingLatest = nil, "", ""
	key, _ := attempt.APIHistoryScope.key()
	if err := saveAPIHistory(ctx, storedb.New(q), attempt.SourceName, key, state); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if attempt.SourceName == "api-user" && attempt.Since == "" {
		if err := resolveCoveredAPIHistory(ctx, q, attempt); err != nil {
			return err
		}
	}
	return commit()
}

func checkAPIHistory(ctx context.Context, q storedb.DBTX, attempt *APIHistoryAttempt) (APIHistoryState, error) {
	if err := ctx.Err(); err != nil {
		return APIHistoryState{}, err
	}
	if attempt == nil {
		return APIHistoryState{}, nil
	}
	key, err := attempt.APIHistoryScope.key()
	if err != nil {
		return APIHistoryState{}, err
	}
	if attempt.Generation == "" {
		return APIHistoryState{}, ErrAPIHistorySuperseded
	}
	if err := apiHistoryOwner(ctx, q, attempt.APIHistoryScope); err != nil {
		return APIHistoryState{}, err
	}
	state, err := loadAPIHistory(ctx, storedb.New(q), attempt.SourceName, key)
	if err != nil {
		return APIHistoryState{}, err
	}
	if state.Pending == nil || state.Generation != attempt.Generation {
		return APIHistoryState{}, ErrAPIHistorySuperseded
	}
	return state, nil
}

func apiHistoryOwner(ctx context.Context, q storedb.DBTX, scope APIHistoryScope) error {
	owner, err := storedb.New(q).GetChannelWorkspace(ctx, scope.ChannelID)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && owner != scope.WorkspaceID) {
		return errors.New("API history requires an existing channel owned by the selected workspace")
	}
	return err
}

func loadAPIHistory(ctx context.Context, q *storedb.Queries, source, key string) (APIHistoryState, error) {
	raw, err := q.GetSyncState(ctx, storedb.GetSyncStateParams{SourceName: source, EntityType: APIHistoryEntityType, EntityID: key})
	if errors.Is(err, sql.ErrNoRows) {
		return APIHistoryState{}, nil
	}
	if err != nil {
		return APIHistoryState{}, err
	}
	return decodeAPIHistory(raw)
}

func decodeAPIHistory(raw string) (APIHistoryState, error) {
	var state APIHistoryState
	if json.Unmarshal([]byte(raw), &state) != nil {
		return APIHistoryState{}, errors.New("invalid API history checkpoint")
	}
	canonical, err := json.Marshal(state)
	if err != nil || !bytes.Equal(canonical, []byte(raw)) || state.Complete != (state.Latest != "") ||
		(state.Generation == "") != (state.PendingLatest == "") || (state.Generation != "" && state.Pending == nil) {
		return APIHistoryState{}, errors.New("invalid API history checkpoint")
	}
	values := []string{state.Latest, state.PendingLatest}
	if state.Pending != nil {
		values = append(values, *state.Pending)
	}
	for _, value := range values {
		if value != "" {
			if _, err := apiHistoryTimestamp(value); err != nil {
				return APIHistoryState{}, errors.New("invalid API history checkpoint")
			}
		}
	}
	if state.PendingLatest != "" && state.Latest != "" {
		pending, _ := apiHistoryTimestamp(state.PendingLatest)
		latest, _ := apiHistoryTimestamp(state.Latest)
		if pending < latest {
			return APIHistoryState{}, errors.New("invalid API history checkpoint")
		}
	}
	return state, nil
}

func saveAPIHistory(ctx context.Context, q *storedb.Queries, source, key string, state APIHistoryState) error {
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return q.SetSyncState(ctx, storedb.SetSyncStateParams{SourceName: source, EntityType: APIHistoryEntityType,
		EntityID: key, Value: string(raw), UpdatedAt: formatDBTime(time.Now().UTC())})
}

func apiHistoryTimestamp(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if value == "" || strings.TrimSpace(value) != value || err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, errors.New("API history requires a finite timestamp")
	}
	return parsed, nil
}

// HasIncompleteAPIHistory evaluates retained API records in one read snapshot.
// Empty workspaceID selects the archive. Missing records do not prove completion.
func (s *Store) HasIncompleteAPIHistory(ctx context.Context, workspaceID string) (bool, error) {
	return hasIncompleteAPIHistory(ctx, s.db, workspaceID)
}

func apiHistoryCoverageRecords(ctx context.Context, q storedb.DBTX, workspaceID, prefix string) ([]apiHistoryCoverageRecord, error) {
	rows, err := q.QueryContext(ctx, `select source_name, entity_id, value, updated_at,
 (select r.value from sync_state r where r.source_name=h.source_name and r.entity_id=h.entity_id and r.entity_type='history_resolution_v1')
 from sync_state h
where source_name in ('api-bot', 'api-user') and entity_type = ? and (? = '' or substr(h.entity_id,1,length(?)) = ?)
order by source_name, entity_id`, APIHistoryEntityType, prefix, prefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []apiHistoryCoverageRecord
	for rows.Next() {
		var source, key, raw, updatedAt string
		var resolution sql.NullString
		if err := rows.Scan(&source, &key, &raw, &updatedAt, &resolution); err != nil {
			return nil, err
		}
		scope, err := decodeAPIHistoryKey(source, key)
		if err != nil {
			if prefix != "" {
				continue
			} // A completion can only resolve valid candidates. Status audits all rows.
			return nil, err
		}
		// An untrusted key cannot establish workspace ownership. Once canonical,
		// a foreign value cannot veto this workspace's cleanup or repair.
		if workspaceID != "" && scope.WorkspaceID != workspaceID {
			continue
		}
		state, err := decodeAPIHistory(raw)
		if err != nil {
			if prefix != "" {
				continue
			} // A completion can only resolve valid candidates. Status audits all rows.
			return nil, err
		}
		records = append(records, apiHistoryCoverageRecord{
			source: source, scope: scope, state: state, updatedAt: updatedAt, raw: raw, resolution: resolution.String,
		})
	}
	// Do not return early on pending work: later malformed records still veto
	// the decision, and must never be hidden by SQL JSON filters or joins.
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func hasIncompleteAPIHistory(ctx context.Context, q storedb.DBTX, workspaceID string) (bool, error) {
	records, err := apiHistoryCoverageRecords(ctx, q, workspaceID, "")
	if err != nil {
		return false, err
	}
	for _, record := range records {
		if record.state.Complete && record.state.Pending == nil {
			continue
		}
		resolved, err := apiHistoryResolved(record)
		if err != nil {
			return false, err
		}
		if resolved {
			continue
		}
		return true, nil
	}
	return false, nil
}

type apiHistoryCoverageRecord struct {
	raw        string
	resolution string
	source     string
	scope      APIHistoryScope
	state      APIHistoryState
	updatedAt  string
}

// Resolution is recorded only while committing a successful scan whose actual
// requested interval contains the unfinished interval. A newer horizon alone
// says nothing about data below a retention floor or incremental lower bound.
func resolveCoveredAPIHistory(ctx context.Context, q storedb.DBTX, attempt APIHistoryAttempt) error {
	if attempt.EnforceRetention {
		floor, err := retentionFloor(ctx, q, attempt.WorkspaceID, attempt.ChannelID)
		if err != nil {
			return err
		}
		attempt.Oldest = (ChannelSyncCursor{RetentionFloor: floor}).ApplyRetentionFloor(attempt.Oldest)
	}
	identity, err := json.Marshal([2]string{attempt.WorkspaceID, attempt.ChannelID})
	if err != nil {
		return err
	}
	prefix := string(identity[:len(identity)-1]) + ","
	records, err := apiHistoryCoverageRecords(ctx, q, attempt.WorkspaceID, prefix)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.scope.ChannelID != attempt.ChannelID || (record.state.Complete && record.state.Pending == nil) ||
			(record.source == "api-user" && record.scope.Since == "") || !apiHistoryIntervalCovers(record, attempt.Oldest, attempt.Latest) {
			continue
		}
		key, _ := record.scope.key()
		proof := apiHistoryResolution{CheckpointSHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(record.raw))),
			CheckpointUpdatedAt: record.updatedAt, Oldest: attempt.Oldest, Latest: attempt.Latest}
		raw, err := json.Marshal(proof)
		if err != nil {
			return err
		}
		if err := storedb.New(q).SetSyncState(ctx, storedb.SetSyncStateParams{SourceName: record.source,
			EntityType: APIHistoryResolutionEntityType, EntityID: key, Value: string(raw), UpdatedAt: formatDBTime(time.Now().UTC())}); err != nil {
			return err
		}
	}
	return nil
}

func apiHistoryResolved(record apiHistoryCoverageRecord) (bool, error) {
	if record.resolution == "" || (record.source == "api-user" && record.scope.Since == "") {
		return false, nil
	}
	var proof apiHistoryResolution
	if json.Unmarshal([]byte(record.resolution), &proof) != nil {
		return false, errors.New("invalid API history resolution")
	}
	canonical, err := json.Marshal(proof)
	if err != nil || string(canonical) != record.resolution || len(proof.CheckpointSHA256) != 64 || parseDBTime(proof.CheckpointUpdatedAt).IsZero() {
		return false, errors.New("invalid API history resolution")
	}
	if _, err := apiHistoryTimestamp(proof.Latest); err != nil {
		return false, err
	}
	if proof.Oldest != "" {
		if _, err := apiHistoryTimestamp(proof.Oldest); err != nil {
			return false, err
		}
	}
	return proof.CheckpointSHA256 == fmt.Sprintf("%x", sha256.Sum256([]byte(record.raw))) &&
		proof.CheckpointUpdatedAt == record.updatedAt && apiHistoryIntervalCovers(record, proof.Oldest, proof.Latest), nil
}

func apiHistoryIntervalCovers(record apiHistoryCoverageRecord, oldest, latest string) bool {
	if record.state.Pending == nil {
		return false
	}
	horizon, ok := apiHistoryIncompleteHorizon(record)
	upper, err := apiHistoryTimestamp(latest)
	if !ok || err != nil || upper < horizon {
		return false
	}
	if oldest == "" {
		return true
	}
	if *record.state.Pending == "" {
		return false
	}
	lower, err := apiHistoryTimestamp(oldest)
	target, targetErr := apiHistoryTimestamp(*record.state.Pending)
	return err == nil && targetErr == nil && lower <= target
}

func apiHistoryIncompleteHorizon(record apiHistoryCoverageRecord) (float64, bool) {
	var horizon float64
	found := false
	for _, value := range []string{record.state.Latest, record.state.PendingLatest} {
		if value == "" {
			continue
		}
		parsed, err := apiHistoryTimestamp(value)
		if err != nil {
			return 0, false
		}
		if !found || parsed > horizon {
			horizon, found = parsed, true
		}
	}
	return horizon, found
}

func decodeAPIHistoryKey(source, key string) (APIHistoryScope, error) {
	var parts [3]string
	if json.Unmarshal([]byte(key), &parts) == nil {
		scope := APIHistoryScope{SourceName: source, WorkspaceID: parts[0], ChannelID: parts[1], Since: parts[2]}
		if canonical, err := scope.key(); err == nil && canonical == key {
			return scope, nil
		}
	}
	return APIHistoryScope{}, errors.New("invalid API history checkpoint key")
}
