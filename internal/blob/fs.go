package blob

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound    = errors.New("no such object")
	ErrNoUpload    = errors.New("no such multipart upload")
	ErrInvalidKey  = errors.New("invalid object key")
	ErrInvalidPart = errors.New("invalid part")
	ErrTooLarge    = errors.New("body too large")
)

var validKey = regexp.MustCompile(`^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$`)

func checkKey(key string) error {
	if !validKey.MatchString(key) || strings.Contains(key, "..") {
		return ErrInvalidKey
	}
	return nil
}

type Part struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}

type Object struct {
	Key  string `json:"key"`
	ETag string `json:"etag"`
	Size int64  `json:"size"`
}

// FS 把物件存在目錄裡。寫入一律先寫暫存檔、fsync、再 rename：讀取的人只會看到完整的物件，
// 同一個 key 被重複寫入（例如轉碼重試）也只是整個換掉。
type FS struct {
	root    string
	minPart int64
	mu      sync.Mutex // 保護 multipart upload 的完成狀態
}

func NewFS(root string, minPart int64) (*FS, error) {
	for _, d := range []string{"objects", "multipart", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	return &FS{root: root, minPart: minPart}, nil
}

func (f *FS) objectPath(key string) string {
	return filepath.Join(f.root, "objects", filepath.FromSlash(key))
}

func (f *FS) uploadDir(id string) string {
	return filepath.Join(f.root, "multipart", id)
}

// writeFile 寫入 r 的內容（最多 limit bytes），回傳 MD5 與大小。
func (f *FS) writeFile(dst string, r io.Reader, limit int64) (string, int64, error) {
	tmp, err := os.CreateTemp(filepath.Join(f.root, "tmp"), "put-*")
	if err != nil {
		return "", 0, err
	}
	defer os.Remove(tmp.Name())
	h := md5.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), io.LimitReader(r, limit+1))
	if err == nil && n > limit {
		err = ErrTooLarge
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return "", 0, err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", 0, err
	}
	if err := retry(func() error { return os.Rename(tmp.Name(), dst) }); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// retry 處理 Windows 上的檔案共用衝突：檔案正被讀取時 rename / remove 會短暫失敗，正被取代時讀取也會。
func retry(fn func() error) error {
	var err error
	for range 20 {
		if err = fn(); err == nil || errors.Is(err, os.ErrNotExist) {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
	return err
}

func (f *FS) Put(key string, r io.Reader, limit int64) (Object, error) {
	if err := checkKey(key); err != nil {
		return Object{}, err
	}
	etag, n, err := f.writeFile(f.objectPath(key), r, limit)
	return Object{Key: key, ETag: etag, Size: n}, err
}

func (f *FS) Open(key string) (*os.File, error) {
	if err := checkKey(key); err != nil {
		return nil, err
	}
	file, err := os.Open(f.objectPath(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return file, err
}

// DeletePrefix 刪掉 prefix 底下的所有物件，回傳刪掉的數量。
func (f *FS) DeletePrefix(prefix string) (int, error) {
	if err := checkKey(prefix); err != nil {
		return 0, err
	}
	dir := f.objectPath(prefix)
	n := 0
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, retry(func() error { return os.RemoveAll(dir) })
}

func (f *FS) CreateMultipart(key string) (string, error) {
	if err := checkKey(key); err != nil {
		return "", err
	}
	b := make([]byte, 12)
	rand.Read(b)
	id := hex.EncodeToString(b)
	if err := os.MkdirAll(f.uploadDir(id), 0o755); err != nil {
		return "", err
	}
	return id, os.WriteFile(filepath.Join(f.uploadDir(id), "key"), []byte(key), 0o644)
}

func (f *FS) uploadKey(id string) (string, error) {
	if !validKey.MatchString(id) {
		return "", ErrNoUpload
	}
	key, err := os.ReadFile(filepath.Join(f.uploadDir(id), "key"))
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNoUpload
	}
	return string(key), err
}

// PutPart 寫入一個 part；同一個 part number 重複上傳時以最後一次為準，和 S3 一樣。
func (f *FS) PutPart(key, id string, n int, r io.Reader, limit int64) (Part, error) {
	if stored, err := f.uploadKey(id); err != nil || stored != key {
		return Part{}, ErrNoUpload
	}
	if n < 1 || n > 10000 {
		return Part{}, ErrInvalidPart
	}
	etag, size, err := f.writeFile(f.partPath(id, n), r, limit)
	if err != nil {
		return Part{}, err
	}
	// 另外記下 ETag 與大小，ListParts 不必重讀每個 part 的內容。資料先落地、再寫這份紀錄：
	// 中間當機的話，這個 part 只會被當成沒上傳過。
	p := Part{PartNumber: n, ETag: etag, Size: size}
	meta, _ := json.Marshal(p)
	if _, _, err := f.writeFile(f.partPath(id, n)+".json", strings.NewReader(string(meta)), 1<<10); err != nil {
		return Part{}, err
	}
	return p, nil
}

func (f *FS) partPath(id string, n int) string {
	return filepath.Join(f.uploadDir(id), fmt.Sprintf("part-%05d", n))
}

func (f *FS) ListParts(key, id string) ([]Part, error) {
	if stored, err := f.uploadKey(id); err != nil || stored != key {
		return nil, ErrNoUpload
	}
	entries, err := os.ReadDir(f.uploadDir(id))
	if err != nil {
		return nil, err
	}
	var parts []Part
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "part-") || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		var data []byte
		err := retry(func() error {
			var err error
			data, err = os.ReadFile(filepath.Join(f.uploadDir(id), e.Name()))
			return err
		})
		if err != nil {
			return nil, err
		}
		var p Part
		if err := json.Unmarshal(data, &p); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, nil
}

// Complete 依 part number 順序把 part 接成最終物件。呼叫者列出的 ETag 必須和儲存的一致，
// 除了最後一個 part 之外都不能小於最小 part 大小。重複呼叫回傳同樣的結果。
// 最終物件的 ETag 仿照 S3 multipart 物件的格式：各 part 的 MD5 串接後再取一次 MD5，加上 -part 數。
func (f *FS) Complete(key, id string, want []Part) (Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if stored, err := f.uploadKey(id); err != nil || stored != key {
		return Object{}, ErrNoUpload
	}
	donePath := filepath.Join(f.uploadDir(id), "done")
	if data, err := os.ReadFile(donePath); err == nil {
		var obj Object
		return obj, json.Unmarshal(data, &obj)
	}
	if len(want) == 0 || !slices.IsSortedFunc(want, func(a, b Part) int { return a.PartNumber - b.PartNumber }) {
		return Object{}, fmt.Errorf("%w: parts must be listed in ascending order", ErrInvalidPart)
	}
	have, err := f.ListParts(key, id)
	if err != nil {
		return Object{}, err
	}
	byNum := make(map[int]Part, len(have))
	for _, p := range have {
		byNum[p.PartNumber] = p
	}
	digests := md5.New()
	readers := make([]io.Reader, 0, len(want))
	var files []*os.File
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()
	for i, w := range want {
		p, ok := byNum[w.PartNumber]
		if !ok || p.ETag != strings.Trim(w.ETag, `"`) {
			return Object{}, fmt.Errorf("%w: part %d is missing or its ETag does not match", ErrInvalidPart, w.PartNumber)
		}
		if i < len(want)-1 && p.Size < f.minPart {
			return Object{}, fmt.Errorf("%w: part %d is %d bytes, below the %d byte minimum", ErrInvalidPart, p.PartNumber, p.Size, f.minPart)
		}
		raw, _ := hex.DecodeString(p.ETag)
		digests.Write(raw)
		file, err := os.Open(f.partPath(id, p.PartNumber))
		if err != nil {
			return Object{}, err
		}
		files = append(files, file)
		readers = append(readers, file)
	}
	_, size, err := f.writeFile(f.objectPath(key), io.MultiReader(readers...), 1<<40)
	if err != nil {
		return Object{}, err
	}
	obj := Object{Key: key, ETag: fmt.Sprintf("%s-%d", hex.EncodeToString(digests.Sum(nil)), len(want)), Size: size}
	data, _ := json.Marshal(obj)
	if _, _, err := f.writeFile(donePath, strings.NewReader(string(data)), 1<<10); err != nil {
		return Object{}, err
	}
	for _, file := range files {
		file.Close()
	}
	files = nil
	for _, p := range have {
		retry(func() error { return os.Remove(f.partPath(id, p.PartNumber)) })
		retry(func() error { return os.Remove(f.partPath(id, p.PartNumber) + ".json") })
	}
	return obj, nil
}
