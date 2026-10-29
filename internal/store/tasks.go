package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"video-platform/internal/domain"
)

// ErrLeaseLost 表示這個 worker 的租約已經過期、任務交給了別人。它的結果不能寫入。
var ErrLeaseLost = errors.New("task lease lost")

const (
	taskWaiting  = "waiting"
	taskReady    = "ready"
	taskRunning  = "running"
	taskDone     = "done"
	taskFailed   = "failed"
	taskCanceled = "canceled"
)

// TaskSpec 描述一個要建立的 DAG 節點。After 列出它依賴的節點（可以是同一批新建的）。
type TaskSpec struct {
	ID          string
	Kind        string
	Args        any
	After       []string
	MaxAttempts int
}

// Task 是被領取的任務。Inputs 是它所依賴節點的輸出。
type Task struct {
	ID         string
	WorkflowID string
	Kind       string
	Args       json.RawMessage
	Attempt    int
	MaxAttempt int
	Token      int64
	Inputs     map[string]json.RawMessage
}

func insertTasks(ctx context.Context, tx *sql.Tx, workflowID string, specs []TaskSpec) error {
	now := time.Now().UnixMilli()
	for _, sp := range specs {
		args, err := json.Marshal(sp.Args)
		if err != nil {
			return err
		}
		pending := 0
		for _, dep := range sp.After {
			var st string
			err := tx.QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id = ?`, dep).Scan(&st)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			// 同一批稍後才插入的節點此時查不到，同樣算作還沒完成。
			if st != taskDone {
				pending++
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO task_deps (task_id, dependent_id) VALUES (?, ?)`, dep, sp.ID); err != nil {
				return err
			}
		}
		status := taskWaiting
		if pending == 0 {
			status = taskReady
		}
		// not_before 記錄任務可以開始執行的時間，Lease 依它排隊；等待中的任務在前置完成時更新。
		if _, err := tx.ExecContext(ctx, `INSERT INTO tasks (task_id, workflow_id, kind, args, status, deps_left, max_attempts, not_before, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, sp.ID, workflowID, sp.Kind, string(args), status, pending, max(sp.MaxAttempts, 1), now, now); err != nil {
			return err
		}
	}
	return nil
}

// Lease 領取一個可以執行的任務：ready 而且到了 not_before，或是 running 但租約已經過期（worker 當機）。
// attempts 與 lease_token 各加一；之後的 Renew、Complete、Fail 都必須帶著這個 token。
func (s *Store) Lease(ctx context.Context, owner string, ttl time.Duration) (Task, bool, error) {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, false, err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	// 兩種候選依「可以執行的時間」一起排隊：ready 的從 not_before 起算，租約過期的從 lease_until 起算。
	// 只在沒有 ready 任務時才回收過期租約的話，尖峰期間當機 worker 手上的任務（可能正卡住整支影片）會一直沒人接手。
	var id string
	var since int64
	err = tx.QueryRowContext(ctx, `SELECT task_id, since FROM (
			SELECT task_id, not_before AS since FROM (
				SELECT task_id, not_before FROM tasks WHERE status = ? AND not_before <= ? ORDER BY not_before, created_at LIMIT 1)
			UNION ALL
			SELECT task_id, lease_until AS since FROM (
				SELECT task_id, lease_until FROM tasks WHERE status = ? AND lease_until < ? ORDER BY lease_until LIMIT 1))
		ORDER BY since LIMIT 1`, taskReady, now, taskRunning, now).Scan(&id, &since)
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, err
	}
	t := Task{ID: id}
	var args string
	err = tx.QueryRowContext(ctx, `UPDATE tasks SET status = ?, lease_owner = ?, lease_token = lease_token + 1, lease_until = ?, attempts = attempts + 1
		WHERE task_id = ? RETURNING workflow_id, kind, args, attempts, max_attempts, lease_token`,
		taskRunning, owner, now+ttl.Milliseconds(), id).Scan(&t.WorkflowID, &t.Kind, &args, &t.Attempt, &t.MaxAttempt, &t.Token)
	if err != nil {
		return Task{}, false, err
	}
	t.Args = json.RawMessage(args)
	t.Inputs, err = inputs(ctx, tx, id)
	if err != nil {
		return Task{}, false, err
	}
	return t, true, tx.Commit()
}

func inputs(ctx context.Context, tx *sql.Tx, id string) (map[string]json.RawMessage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT t.task_id, t.result FROM task_deps d JOIN tasks t ON t.task_id = d.task_id WHERE d.dependent_id = ?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var dep, result string
		if err := rows.Scan(&dep, &result); err != nil {
			return nil, err
		}
		out[dep] = json.RawMessage(result)
	}
	return out, rows.Err()
}

// Renew 延長租約。長時間的任務定期呼叫，回傳 ErrLeaseLost 時 worker 應該放棄這個任務。
func (s *Store) Renew(ctx context.Context, id string, token int64, ttl time.Duration) error {
	res, err := s.w.ExecContext(ctx, `UPDATE tasks SET lease_until = ? WHERE task_id = ? AND status = ? AND lease_token = ?`,
		time.Now().Add(ttl).UnixMilli(), id, taskRunning, token)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Complete 在一個交易裡：標記任務完成、讓依賴它的節點少一個未完成的前置、建立任務執行時新產生的節點。
// 只有持有目前 token 的 worker 能完成任務：過期的 worker 晚一步回來，不會讓下游的計數被扣兩次。
func (s *Store) Complete(ctx context.Context, id string, token int64, result any, spawn []TaskSpec) error {
	out, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var workflowID string
	err = tx.QueryRowContext(ctx, `UPDATE tasks SET status = ?, result = ?, lease_owner = '', error = ''
		WHERE task_id = ? AND status = ? AND lease_token = ? RETURNING workflow_id`, taskDone, string(out), id, taskRunning, token).Scan(&workflowID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return err
	}
	// 先處理既有的下游，再建立新節點：新節點計算 deps_left 時已經把這個任務當成完成，不能再扣一次。
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET deps_left = deps_left - 1,
		not_before = CASE WHEN deps_left = 1 AND status = ? THEN ? ELSE not_before END,
		status = CASE WHEN deps_left = 1 AND status = ? THEN ? ELSE status END
		WHERE task_id IN (SELECT dependent_id FROM task_deps WHERE task_id = ?)`, taskWaiting, time.Now().UnixMilli(), taskWaiting, taskReady, id); err != nil {
		return err
	}
	if err := insertTasks(ctx, tx, workflowID, spawn); err != nil {
		return err
	}
	return tx.Commit()
}

// Fail 回報任務失敗。可以重試而且次數未滿時，任務在 retryAt 之後重新變成 ready；
// 否則任務失敗，同一個 workflow 裡還沒開始的節點全部取消，影片也在同一個交易裡標成 failed。
func (s *Store) Fail(ctx context.Context, id string, token int64, reason string, retryAt time.Time, permanent bool) (bool, error) {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var workflowID string
	var attempts, maxAttempts int
	err = tx.QueryRowContext(ctx, `SELECT workflow_id, attempts, max_attempts FROM tasks WHERE task_id = ? AND status = ? AND lease_token = ?`,
		id, taskRunning, token).Scan(&workflowID, &attempts, &maxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return false, ErrLeaseLost
	}
	if err != nil {
		return false, err
	}
	if !permanent && attempts < maxAttempts {
		_, err := tx.ExecContext(ctx, `UPDATE tasks SET status = ?, not_before = ?, error = ?, lease_owner = '' WHERE task_id = ?`,
			taskReady, retryAt.UnixMilli(), reason, id)
		if err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status = ?, error = ?, lease_owner = '' WHERE task_id = ?`, taskFailed, reason, id); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status = ? WHERE workflow_id = ? AND status IN (?, ?)`,
		taskCanceled, workflowID, taskWaiting, taskReady); err != nil {
		return false, err
	}
	// workflow_id 就是 video_id：任務失敗與影片失敗必須一起寫入，不會留下永遠停在 processing 的影片。
	if _, err := tx.ExecContext(ctx, `UPDATE videos SET status = ?, error = ? WHERE video_id = ? AND status = ?`,
		domain.StatusFailed, reason, workflowID, domain.StatusProcessing); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// TaskCounts 回傳各狀態的任務數，autoscaler 以 ready + running 決定 worker 數。
func (s *Store) TaskCounts(ctx context.Context) (map[string]int, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT status, COUNT(*) FROM tasks GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var st string
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}

// Backlog 回傳等待執行與執行中的任務數。
func (s *Store) Backlog(ctx context.Context) (int, error) {
	var n int
	err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM tasks WHERE status IN (?, ?)`, taskReady, taskRunning).Scan(&n)
	return n, err
}

// TaskRow 是給驗證用的任務狀態。
type TaskRow struct {
	ID         string
	WorkflowID string
	Kind       string
	Status     string
	DepsLeft   int
	Attempts   int
}

func (s *Store) Tasks(ctx context.Context) ([]TaskRow, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT task_id, workflow_id, kind, status, deps_left, attempts FROM tasks`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TaskRow
	for rows.Next() {
		var t TaskRow
		if err := rows.Scan(&t.ID, &t.WorkflowID, &t.Kind, &t.Status, &t.DepsLeft, &t.Attempts); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
