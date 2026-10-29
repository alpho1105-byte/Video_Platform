package blob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client 是內部服務（video service、轉碼 worker）使用的 SDK：每個請求都用服務自己的金鑰簽章，
// 就像 S3 SDK 用 access key 簽 SigV4。
type Client struct {
	base   string
	signer *Signer
	http   *http.Client
}

func NewClient(base string, signer *Signer) *Client {
	return &Client{base: base, signer: signer, http: &http.Client{Timeout: 30 * time.Second}}
}

// Presign 回傳讓 client 直接存取 blob store 的完整網址。
func (c *Client) Presign(method, key string, params url.Values, ttl time.Duration) string {
	return c.base + c.signer.Presign(method, key, params, ttl)
}

// PresignPart 回傳上傳 multipart 某個 part 的網址。產生它不需要呼叫 blob store。
func (c *Client) PresignPart(key, uploadID string, part int, ttl time.Duration) string {
	return c.Presign(http.MethodPut, key, url.Values{"uploadId": {uploadID}, "partNumber": {strconv.Itoa(part)}}, ttl)
}

func (c *Client) do(ctx context.Context, method, key string, params url.Values, body []byte) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Presign(method, key, params, time.Minute), r)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		defer resp.Body.Close()
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil, fmt.Errorf("blob %s %s: HTTP %d: %s", method, key, resp.StatusCode, e.Error)
	}
	return resp, nil
}

func (c *Client) doJSON(ctx context.Context, method, key string, params url.Values, body []byte, out any) error {
	resp, err := c.do(ctx, method, key, params, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) Put(ctx context.Context, key string, data []byte) (string, error) {
	resp, err := c.do(ctx, http.MethodPut, key, nil, data)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// Open 串流讀取物件，呼叫者負責關閉。
func (c *Client) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := c.do(ctx, http.MethodGet, key, nil, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	body, err := c.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

func (c *Client) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	var out struct {
		Deleted int `json:"deleted"`
	}
	err := c.doJSON(ctx, http.MethodDelete, prefix, url.Values{"recursive": {"1"}}, nil, &out)
	return out.Deleted, err
}

func (c *Client) CreateMultipart(ctx context.Context, key string) (string, error) {
	var out struct {
		UploadID string `json:"upload_id"`
	}
	err := c.doJSON(ctx, http.MethodPost, key, url.Values{"uploads": {""}}, nil, &out)
	return out.UploadID, err
}

func (c *Client) ListParts(ctx context.Context, key, uploadID string) ([]Part, error) {
	var out struct {
		Parts []Part `json:"parts"`
	}
	err := c.doJSON(ctx, http.MethodGet, key, url.Values{"uploadId": {uploadID}}, nil, &out)
	return out.Parts, err
}

func (c *Client) Complete(ctx context.Context, key, uploadID string, parts []Part) (Object, error) {
	body, _ := json.Marshal(map[string][]Part{"parts": parts})
	var obj Object
	err := c.doJSON(ctx, http.MethodPost, key, url.Values{"uploadId": {uploadID}}, body, &obj)
	return obj, err
}
