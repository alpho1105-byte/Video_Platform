package blob

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
)

// PublicPrefix 底下的物件（轉碼後的段落與 manifest）任何人都能讀，給 CDN 當 origin；其他物件都要簽章。
const PublicPrefix = "vod/"

type ServerConfig struct {
	MaxPart   int64
	MaxObject int64
}

type ServerStats struct {
	Gets      atomic.Int64
	BytesOut  atomic.Int64
	PartPuts  atomic.Int64
	Forbidden atomic.Int64
}

type Server struct {
	fs     *FS
	signer *Signer
	cfg    ServerConfig
	stats  ServerStats
}

func NewServer(fs *FS, signer *Signer, cfg ServerConfig) *Server {
	return &Server{fs: fs, signer: signer, cfg: cfg}
}

func (s *Server) Stats() *ServerStats { return &s.stats }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/o/{key...}", s.handle)
	return mux
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	q := r.URL.Query()
	read := r.Method == http.MethodGet || r.Method == http.MethodHead
	public := read && strings.HasPrefix(key, PublicPrefix) && !q.Has("uploadId")
	if !public {
		if err := s.signer.Verify(r.Method, key, q); err != nil {
			s.stats.Forbidden.Add(1)
			writeError(w, http.StatusForbidden, err)
			return
		}
	}
	switch {
	case r.Method == http.MethodPut && q.Has("uploadId"):
		n := atoi(q.Get("partNumber"))
		p, err := s.fs.PutPart(key, q.Get("uploadId"), n, r.Body, s.cfg.MaxPart)
		if err != nil {
			s.fail(w, err)
			return
		}
		s.stats.PartPuts.Add(1)
		w.Header().Set("ETag", `"`+p.ETag+`"`)
	case r.Method == http.MethodPut:
		obj, err := s.fs.Put(key, r.Body, s.cfg.MaxObject)
		if err != nil {
			s.fail(w, err)
			return
		}
		w.Header().Set("ETag", `"`+obj.ETag+`"`)
	case r.Method == http.MethodPost && q.Has("uploads"):
		id, err := s.fs.CreateMultipart(key)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, map[string]string{"upload_id": id})
	case r.Method == http.MethodPost && q.Has("uploadId"):
		var req struct {
			Parts []Part `json:"parts"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		obj, err := s.fs.Complete(key, q.Get("uploadId"), req.Parts)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, obj)
	case r.Method == http.MethodGet && q.Has("uploadId"):
		parts, err := s.fs.ListParts(key, q.Get("uploadId"))
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, map[string][]Part{"parts": parts})
	case read:
		f, err := s.fs.Open(key)
		if err != nil {
			s.fail(w, err)
			return
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil {
			s.fail(w, err)
			return
		}
		if public {
			// 轉碼後的段落永遠不會改變（同一個 key 重寫的內容也一樣），CDN 與瀏覽器可以一直快取。
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		s.stats.Gets.Add(1)
		s.stats.BytesOut.Add(info.Size())
		http.ServeContent(w, r, "", info.ModTime(), f)
	case r.Method == http.MethodDelete && q.Get("recursive") == "1":
		n, err := s.fs.DeletePrefix(key)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, map[string]int{"deleted": n})
	default:
		writeError(w, http.StatusMethodNotAllowed, errors.New("unsupported operation"))
	}
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrNoUpload):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, ErrInvalidKey), errors.Is(err, ErrInvalidPart):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, ErrTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' || n > 1_000_000 {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
