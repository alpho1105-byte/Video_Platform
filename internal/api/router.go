// Package api 是 Video Service 的 HTTP 介面。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"video-platform/internal/auth"
	"video-platform/internal/domain"
	"video-platform/internal/videos"
)

type Deps struct {
	Signer *auth.Signer
	Videos *videos.Service
	// Dev 開啟 POST /dev/token，方便本機測試時取得 token。
	Dev bool
	Log *slog.Logger
}

func NewRouter(d Deps) http.Handler {
	h := &handler{d: d}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/videos", h.auth(h.create))
	mux.HandleFunc("POST /v1/videos/{id}/upload-urls", h.auth(h.uploadURLs))
	mux.HandleFunc("POST /v1/videos/{id}/parts", h.auth(h.confirmPart))
	mux.HandleFunc("GET /v1/videos/{id}/upload", h.auth(h.uploadStatus))
	mux.HandleFunc("POST /v1/videos/{id}/complete", h.auth(h.complete))
	mux.HandleFunc("GET /v1/videos/{id}", h.get)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	if d.Dev {
		mux.HandleFunc("POST /dev/token", h.token)
	}
	return mux
}

type handler struct {
	d Deps
}

type userHandler func(w http.ResponseWriter, r *http.Request, user string)

func (h *handler) user(r *http.Request) (string, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return "", false
	}
	user, err := h.d.Signer.Verify(token)
	return user, err == nil
}

func (h *handler) auth(next userHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, ok := h.user(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "invalid or missing token")
			return
		}
		next(w, r, user)
	}
}

func (h *handler) create(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		SizeBytes   int64  `json:"size_bytes"`
	}
	if !decode(w, r, &req) {
		return
	}
	v, err := h.d.Videos.Create(r.Context(), user, req.Title, req.Description, req.SizeBytes)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"video_id": v.ID, "status": v.Status, "part_size": v.PartSize, "part_count": v.PartCount()})
}

func (h *handler) uploadURLs(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		Parts []int `json:"parts"`
	}
	if !decode(w, r, &req) {
		return
	}
	urls, err := h.d.Videos.UploadURLs(r.Context(), user, r.PathValue("id"), req.Parts)
	if err != nil {
		h.fail(w, err)
		return
	}
	out := make(map[string]string, len(urls))
	for n, u := range urls {
		out[strconv.Itoa(n)] = u
	}
	writeJSON(w, http.StatusOK, map[string]any{"urls": out, "expires_in": int(h.d.Videos.Config().URLTTL / time.Second)})
}

func (h *handler) confirmPart(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		PartNumber int    `json:"part_number"`
		ETag       string `json:"etag"`
	}
	if !decode(w, r, &req) {
		return
	}
	if err := h.d.Videos.ConfirmPart(r.Context(), user, r.PathValue("id"), req.PartNumber, req.ETag); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) uploadStatus(w http.ResponseWriter, r *http.Request, user string) {
	v, parts, err := h.d.Videos.Parts(r.Context(), user, r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	if parts == nil {
		parts = []domain.Part{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"video_id": v.ID, "status": v.Status, "part_size": v.PartSize,
		"part_count": v.PartCount(), "parts": parts})
}

func (h *handler) complete(w http.ResponseWriter, r *http.Request, user string) {
	v, err := h.d.Videos.Complete(r.Context(), user, r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"video_id": v.ID, "status": v.Status})
}

// get 回傳影片 metadata。還沒處理完的影片只有上傳者看得到。
func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	v, err := h.d.Videos.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	user, _ := h.user(r)
	if v.Status != domain.StatusReady && v.Uploader != user {
		writeError(w, http.StatusNotFound, "video not found")
		return
	}
	out := map[string]any{"video_id": v.ID, "title": v.Title, "description": v.Description, "uploader": v.Uploader,
		"status": v.Status, "duration_ms": v.DurationMs, "renditions": v.Renditions}
	if v.Status == domain.StatusReady {
		// manifest 是 CDN 上的路徑，client 依自己所在的區域連到最近的 edge。
		out["manifest"] = "/" + v.Manifest
	}
	if v.Error != "" && v.Uploader == user {
		out["error"] = v.Error
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *handler) token(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
	}
	if !decode(w, r, &req) || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "user_id is required")
		return
	}
	tok, err := h.d.Signer.Issue(req.UserID, 24*time.Hour)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "issue token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": tok})
}

func (h *handler) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.Canceled):
		// client 已經斷線（例如上傳中途被關掉），沒有人會收到回應。
		return
	case errors.Is(err, videos.ErrInvalid):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, videos.ErrPartMismatch), errors.Is(err, videos.ErrIncomplete), errors.Is(err, domain.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "video not found")
	default:
		h.d.Log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
