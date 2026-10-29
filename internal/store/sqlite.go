package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const schema = `
-- 影片 metadata。正式環境用以 video_id 分區的 NoSQL（例如 Cassandra），這裡所有查詢也都以 video_id 為鍵。
CREATE TABLE IF NOT EXISTS videos (
	video_id    TEXT PRIMARY KEY,
	uploader    TEXT NOT NULL,
	title       TEXT NOT NULL,
	description TEXT NOT NULL,
	size_bytes  INTEGER NOT NULL,
	part_size   INTEGER NOT NULL,
	upload_id   TEXT NOT NULL,
	raw_key     TEXT NOT NULL,
	raw_etag    TEXT NOT NULL DEFAULT '',
	status      TEXT NOT NULL,
	duration_ms INTEGER NOT NULL DEFAULT 0,
	renditions  TEXT NOT NULL DEFAULT '[]',
	manifest    TEXT NOT NULL DEFAULT '',
	error       TEXT NOT NULL DEFAULT '',
	created_at  INTEGER NOT NULL
);

-- 每個已確認的 part 一列。client 中斷後重新查詢，就知道哪些 part 可以跳過。
CREATE TABLE IF NOT EXISTS video_parts (
	video_id    TEXT NOT NULL,
	part_number INTEGER NOT NULL,
	etag        TEXT NOT NULL,
	size        INTEGER NOT NULL,
	PRIMARY KEY (video_id, part_number)
) WITHOUT ROWID;

-- 轉檔 DAG 的節點。workflow_id 就是 video_id。deps_left 歸零的節點變成 ready；
-- lease_token 每次被領取就加一，完成或回報失敗時要帶著它，過期的 worker 因此無法覆寫結果。
CREATE TABLE IF NOT EXISTS tasks (
	task_id      TEXT PRIMARY KEY,
	workflow_id  TEXT NOT NULL,
	kind         TEXT NOT NULL,
	args         TEXT NOT NULL,
	status       TEXT NOT NULL,
	deps_left    INTEGER NOT NULL,
	attempts     INTEGER NOT NULL DEFAULT 0,
	max_attempts INTEGER NOT NULL,
	not_before   INTEGER NOT NULL DEFAULT 0,
	lease_owner  TEXT NOT NULL DEFAULT '',
	lease_token  INTEGER NOT NULL DEFAULT 0,
	lease_until  INTEGER NOT NULL DEFAULT 0,
	result       TEXT NOT NULL DEFAULT '',
	error        TEXT NOT NULL DEFAULT '',
	created_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_ready ON tasks (status, not_before);
CREATE INDEX IF NOT EXISTS tasks_leased ON tasks (status, lease_until);
CREATE INDEX IF NOT EXISTS tasks_workflow ON tasks (workflow_id, status);

CREATE TABLE IF NOT EXISTS task_deps (
	task_id      TEXT NOT NULL,
	dependent_id TEXT NOT NULL,
	PRIMARY KEY (task_id, dependent_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS task_deps_dependent ON task_deps (dependent_id);
`

// Store 保存影片 metadata 與轉檔 DAG。寫入走單一連線（SQLite 同時只允許一個 writer），讀取另開連線池。
type Store struct {
	w *sql.DB
	r *sql.DB
}

// Open 開啟資料庫。synchronous=FULL：commit 回傳前已經 fsync，回覆過「完成」的狀態不會因斷電遺失。
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	base := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	w, err := open(base+"&_pragma=synchronous(FULL)", 1)
	if err != nil {
		return nil, err
	}
	if _, err := w.Exec(schema); err != nil {
		w.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	r, err := open(base+"&_pragma=query_only(1)", 8)
	if err != nil {
		w.Close()
		return nil, err
	}
	return &Store{w: w, r: r}, nil
}

func open(dsn string, conns int) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return db, nil
}

func (s *Store) Close() error {
	s.r.Close()
	return s.w.Close()
}
