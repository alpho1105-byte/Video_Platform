package domain

import (
	"errors"
	"time"
)

var (
	ErrNotFound  = errors.New("not found")
	ErrForbidden = errors.New("forbidden")
	ErrConflict  = errors.New("conflict")
)

type Status string

const (
	StatusUploading  Status = "uploading"
	StatusProcessing Status = "processing"
	StatusReady      Status = "ready"
	StatusFailed     Status = "failed"
)

// Final 表示影片狀態不會再改變，可以放心快取。
func (s Status) Final() bool { return s == StatusReady || s == StatusFailed }

type Video struct {
	ID          string
	Uploader    string
	Title       string
	Description string
	Size        int64
	PartSize    int64
	UploadID    string
	RawKey      string
	RawETag     string
	Status      Status
	DurationMs  int64
	Renditions  []string
	Manifest    string
	Error       string
	CreatedAt   time.Time
}

// PartCount 是依宣告大小切出的 part 數。
func (v Video) PartCount() int {
	return int((v.Size + v.PartSize - 1) / v.PartSize)
}

// PartLen 是第 n 個 part（從 1 開始）應有的大小：最後一個 part 是剩下的部分。
func (v Video) PartLen(n int) int64 {
	if n == v.PartCount() {
		return v.Size - int64(n-1)*v.PartSize
	}
	return v.PartSize
}

// Part 是一個已經確認上傳完成的 part。
type Part struct {
	Number int    `json:"part_number"`
	ETag   string `json:"etag"`
	Size   int64  `json:"size"`
}
