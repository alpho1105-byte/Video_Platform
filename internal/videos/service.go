// Package videos 是 Video Service：無狀態，處理上傳流程與 metadata 查詢。影片內容從不經過這裡，
// client 拿 presigned URL 直接對 blob store 上傳。
package videos

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"video-platform/internal/blob"
	"video-platform/internal/domain"
	"video-platform/internal/store"
)

var (
	ErrInvalid      = errors.New("invalid request")
	ErrPartMismatch = errors.New("part does not match what the blob store received")
	ErrIncomplete   = errors.New("not every part has been uploaded")
)

type Config struct {
	PartSize int64
	MaxSize  int64
	URLTTL   time.Duration
}

func DefaultConfig() Config {
	return Config{PartSize: 8 << 20, MaxSize: 50 << 30, URLTTL: 15 * time.Minute}
}

// RootTasks 建立一支影片轉檔 DAG 的起點。
type RootTasks func(videoID, rawKey string) []store.TaskSpec

type Service struct {
	cfg   Config
	store *store.Store
	blob  *blob.Client
	root  RootTasks
	cache *Cache
}

func New(cfg Config, st *store.Store, bc *blob.Client, root RootTasks, cache *Cache) *Service {
	return &Service{cfg: cfg, store: st, blob: bc, root: root, cache: cache}
}

func (s *Service) Config() Config { return s.cfg }

// Create 建立影片紀錄並開一個 multipart upload。part 大小由 server 決定，client 依此切檔。
func (s *Service) Create(ctx context.Context, user, title, description string, size int64) (domain.Video, error) {
	title = strings.TrimSpace(title)
	if title == "" || utf8.RuneCountInString(title) > 200 || utf8.RuneCountInString(description) > 5000 || size <= 0 || size > s.cfg.MaxSize {
		return domain.Video{}, ErrInvalid
	}
	b := make([]byte, 8)
	rand.Read(b)
	id := "v_" + hex.EncodeToString(b)
	rawKey := "raw/" + id
	uploadID, err := s.blob.CreateMultipart(ctx, rawKey)
	if err != nil {
		return domain.Video{}, err
	}
	v := domain.Video{ID: id, Uploader: user, Title: title, Description: description, Size: size, PartSize: s.cfg.PartSize,
		UploadID: uploadID, RawKey: rawKey, Status: domain.StatusUploading, CreatedAt: time.Now()}
	return v, s.store.CreateVideo(ctx, v)
}

// owned 回傳使用者自己上傳中的影片；別人的影片一律當作不存在。
func (s *Service) owned(ctx context.Context, user, id string) (domain.Video, error) {
	v, err := s.store.GetVideo(ctx, id)
	if err != nil {
		return domain.Video{}, err
	}
	if v.Uploader != user {
		return domain.Video{}, domain.ErrNotFound
	}
	return v, nil
}

// UploadURLs 為指定的 part 產生 presigned URL。簽章在本地計算，不需要呼叫 blob store。
func (s *Service) UploadURLs(ctx context.Context, user, id string, parts []int) (map[int]string, error) {
	v, err := s.owned(ctx, user, id)
	if err != nil {
		return nil, err
	}
	if v.Status != domain.StatusUploading {
		return nil, domain.ErrConflict
	}
	if len(parts) == 0 || len(parts) > 1000 {
		return nil, ErrInvalid
	}
	out := make(map[int]string, len(parts))
	for _, n := range parts {
		if n < 1 || n > v.PartCount() {
			return nil, fmt.Errorf("%w: part %d of %d", ErrInvalid, n, v.PartCount())
		}
		out[n] = s.blob.PresignPart(v.RawKey, v.UploadID, n, s.cfg.URLTTL)
	}
	return out, nil
}

// ConfirmPart 記錄 client 上傳完成的 part。不相信 client 說的 ETag：向 blob store 查 ListParts 比對，
// 大小也必須剛好是這個 part 該有的大小。
func (s *Service) ConfirmPart(ctx context.Context, user, id string, n int, etag string) error {
	v, err := s.owned(ctx, user, id)
	if err != nil {
		return err
	}
	if v.Status != domain.StatusUploading {
		return domain.ErrConflict
	}
	if n < 1 || n > v.PartCount() {
		return ErrInvalid
	}
	parts, err := s.blob.ListParts(ctx, v.RawKey, v.UploadID)
	if err != nil {
		return err
	}
	for _, p := range parts {
		if p.PartNumber == n {
			if p.ETag != strings.Trim(etag, `"`) || p.Size != v.PartLen(n) {
				return ErrPartMismatch
			}
			return s.store.SavePart(ctx, id, domain.Part{Number: n, ETag: p.ETag, Size: p.Size})
		}
	}
	return ErrPartMismatch
}

// Complete 在所有 part 都確認之後把它們接成最終物件，並在同一個交易裡把影片改成 processing、建立轉檔工作。
// 重複呼叫是安全的：blob store 的 complete 與狀態轉換都是冪等的。
func (s *Service) Complete(ctx context.Context, user, id string) (domain.Video, error) {
	v, err := s.owned(ctx, user, id)
	if err != nil {
		return domain.Video{}, err
	}
	if v.Status != domain.StatusUploading {
		return v, nil
	}
	parts, err := s.store.Parts(ctx, id)
	if err != nil {
		return domain.Video{}, err
	}
	if len(parts) != v.PartCount() {
		return domain.Video{}, fmt.Errorf("%w: %d of %d parts", ErrIncomplete, len(parts), v.PartCount())
	}
	list := make([]blob.Part, len(parts))
	for i, p := range parts {
		list[i] = blob.Part{PartNumber: p.Number, ETag: p.ETag}
	}
	obj, err := s.blob.Complete(ctx, v.RawKey, v.UploadID, list)
	if err != nil {
		return domain.Video{}, err
	}
	if obj.Size != v.Size {
		return domain.Video{}, fmt.Errorf("%w: assembled %d bytes, declared %d", ErrPartMismatch, obj.Size, v.Size)
	}
	if err := s.store.StartProcessing(ctx, id, obj.ETag, s.root(id, v.RawKey)); err != nil && !errors.Is(err, domain.ErrConflict) {
		return domain.Video{}, err
	}
	return s.store.GetVideo(ctx, id)
}

// Get 回傳影片 metadata。狀態已經是最終狀態（ready / failed）的影片不會再變，放進快取擋下熱門影片的大量查詢。
func (s *Service) Get(ctx context.Context, id string) (domain.Video, error) {
	if v, ok := s.cache.Get(id); ok {
		return v, nil
	}
	v, err := s.store.GetVideo(ctx, id)
	if err != nil {
		return domain.Video{}, err
	}
	if v.Status.Final() {
		s.cache.Put(v)
	}
	return v, nil
}

// Parts 回傳已確認的 part，client 中斷後據此跳過已經上傳的部分。
func (s *Service) Parts(ctx context.Context, user, id string) (domain.Video, []domain.Part, error) {
	v, err := s.owned(ctx, user, id)
	if err != nil {
		return domain.Video{}, nil, err
	}
	parts, err := s.store.Parts(ctx, id)
	return v, parts, err
}
