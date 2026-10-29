// Package client 是影音平台的 client：Video Service 的 API、可續傳的上傳器，以及自適應位元率的播放器。
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"video-platform/internal/domain"
)

type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Message) }

// API 呼叫 Video Service。
type API struct {
	Base  string
	Token string
	http  *http.Client
}

func NewAPI(base, token string) *API {
	return &API{Base: base, Token: token, http: &http.Client{Timeout: 30 * time.Second}}
}

// Login 用 /dev/token 取得 token（只在開發模式提供）。
func Login(ctx context.Context, base, userID string) (*API, error) {
	a := NewAPI(base, "")
	var out struct {
		Token string `json:"token"`
	}
	if err := a.do(ctx, http.MethodPost, "/dev/token", map[string]string{"user_id": userID}, &out); err != nil {
		return nil, err
	}
	a.Token = out.Token
	return a, nil
}

type Created struct {
	VideoID   string `json:"video_id"`
	PartSize  int64  `json:"part_size"`
	PartCount int    `json:"part_count"`
}

func (a *API) CreateVideo(ctx context.Context, title, description string, size int64) (Created, error) {
	var out Created
	err := a.do(ctx, http.MethodPost, "/v1/videos", map[string]any{"title": title, "description": description, "size_bytes": size}, &out)
	return out, err
}

func (a *API) UploadURLs(ctx context.Context, videoID string, parts []int) (map[int]string, error) {
	var out struct {
		URLs map[string]string `json:"urls"`
	}
	if err := a.do(ctx, http.MethodPost, "/v1/videos/"+videoID+"/upload-urls", map[string]any{"parts": parts}, &out); err != nil {
		return nil, err
	}
	urls := make(map[int]string, len(out.URLs))
	for k, v := range out.URLs {
		n, _ := strconv.Atoi(k)
		urls[n] = v
	}
	return urls, nil
}

func (a *API) ConfirmPart(ctx context.Context, videoID string, part int, etag string) error {
	return a.do(ctx, http.MethodPost, "/v1/videos/"+videoID+"/parts", map[string]any{"part_number": part, "etag": etag}, nil)
}

type UploadStatus struct {
	Status    domain.Status `json:"status"`
	PartSize  int64         `json:"part_size"`
	PartCount int           `json:"part_count"`
	Parts     []domain.Part `json:"parts"`
}

func (a *API) UploadStatus(ctx context.Context, videoID string) (UploadStatus, error) {
	var out UploadStatus
	err := a.do(ctx, http.MethodGet, "/v1/videos/"+videoID+"/upload", nil, &out)
	return out, err
}

func (a *API) Complete(ctx context.Context, videoID string) (domain.Status, error) {
	var out struct {
		Status domain.Status `json:"status"`
	}
	err := a.do(ctx, http.MethodPost, "/v1/videos/"+videoID+"/complete", nil, &out)
	return out.Status, err
}

type VideoInfo struct {
	ID         string        `json:"video_id"`
	Title      string        `json:"title"`
	Status     domain.Status `json:"status"`
	DurationMs int64         `json:"duration_ms"`
	Renditions []string      `json:"renditions"`
	Manifest   string        `json:"manifest"`
	Error      string        `json:"error"`
}

func (a *API) Video(ctx context.Context, videoID string) (VideoInfo, error) {
	var out VideoInfo
	err := a.do(ctx, http.MethodGet, "/v1/videos/"+videoID, nil, &out)
	return out, err
}

func (a *API) do(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.Base+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.Token)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return &HTTPError{Status: resp.StatusCode, Message: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
