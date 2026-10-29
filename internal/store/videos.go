package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"video-platform/internal/domain"
)

func (s *Store) CreateVideo(ctx context.Context, v domain.Video) error {
	_, err := s.w.ExecContext(ctx, `INSERT INTO videos (video_id, uploader, title, description, size_bytes, part_size, upload_id, raw_key, status, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		v.ID, v.Uploader, v.Title, v.Description, v.Size, v.PartSize, v.UploadID, v.RawKey, v.Status, v.CreatedAt.UnixMilli())
	return err
}

func (s *Store) GetVideo(ctx context.Context, id string) (domain.Video, error) {
	var v domain.Video
	var renditions string
	var created int64
	err := s.r.QueryRowContext(ctx, `SELECT video_id, uploader, title, description, size_bytes, part_size, upload_id, raw_key, raw_etag,
		status, duration_ms, renditions, manifest, error, created_at FROM videos WHERE video_id = ?`, id).
		Scan(&v.ID, &v.Uploader, &v.Title, &v.Description, &v.Size, &v.PartSize, &v.UploadID, &v.RawKey, &v.RawETag,
			&v.Status, &v.DurationMs, &renditions, &v.Manifest, &v.Error, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Video{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Video{}, err
	}
	v.CreatedAt = time.UnixMilli(created)
	return v, json.Unmarshal([]byte(renditions), &v.Renditions)
}

// SavePart 記錄一個已確認的 part；同一個 part 重新上傳時以最新的為準。
func (s *Store) SavePart(ctx context.Context, videoID string, p domain.Part) error {
	_, err := s.w.ExecContext(ctx, `INSERT INTO video_parts (video_id, part_number, etag, size) VALUES (?, ?, ?, ?)
		ON CONFLICT (video_id, part_number) DO UPDATE SET etag = excluded.etag, size = excluded.size`,
		videoID, p.Number, p.ETag, p.Size)
	return err
}

func (s *Store) Parts(ctx context.Context, videoID string) ([]domain.Part, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT part_number, etag, size FROM video_parts WHERE video_id = ? ORDER BY part_number`, videoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Part
	for rows.Next() {
		var p domain.Part
		if err := rows.Scan(&p.Number, &p.ETag, &p.Size); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// StartProcessing 把影片改成 processing，並在同一個交易裡建立轉檔 DAG 的起點：
// 不會出現「狀態改了，轉檔工作卻沒建立」或反過來的情況。影片已經不在 uploading 時回傳 ErrConflict。
func (s *Store) StartProcessing(ctx context.Context, videoID, rawETag string, root []TaskSpec) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `UPDATE videos SET status = ?, raw_etag = ? WHERE video_id = ? AND status = ?`,
		domain.StatusProcessing, rawETag, videoID, domain.StatusUploading)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return domain.ErrConflict
	}
	if err := insertTasks(ctx, tx, videoID, root); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkReady(ctx context.Context, videoID string, durationMs int64, renditions []string, manifest string) error {
	r, _ := json.Marshal(renditions)
	_, err := s.w.ExecContext(ctx, `UPDATE videos SET status = ?, duration_ms = ?, renditions = ?, manifest = ?
		WHERE video_id = ? AND status IN (?, ?)`, domain.StatusReady, durationMs, string(r), manifest,
		videoID, domain.StatusProcessing, domain.StatusReady)
	return err
}

// CountByStatus 回傳各狀態的影片數。
func (s *Store) CountByStatus(ctx context.Context) (map[domain.Status]int, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT status, COUNT(*) FROM videos GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[domain.Status]int{}
	for rows.Next() {
		var st domain.Status
		var n int
		if err := rows.Scan(&st, &n); err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, rows.Err()
}
