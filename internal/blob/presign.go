// Package blob 是 S3 風格的物件儲存：物件存在磁碟上，支援 multipart upload，
// 每個請求都要帶 presigned URL 的簽章（vod/ 前綴除外，它是公開讀取的）。
package blob

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"strconv"
	"time"
)

var (
	ErrBadSignature = errors.New("signature does not match")
	ErrExpired      = errors.New("presigned URL expired")
)

const (
	paramExpires   = "X-Expires"
	paramSignature = "X-Signature"
)

// Signer 產生與驗證 presigned URL。簽章涵蓋 HTTP method、object key、所有查詢參數（例如 uploadId、partNumber）
// 與到期時間：拿到某個 part 的上傳網址，不能改成上傳別的 part、別的物件，或改成 GET。
type Signer struct {
	key []byte
	now func() time.Time
}

func NewSigner(key []byte) *Signer {
	return &Signer{key: key, now: time.Now}
}

// Presign 回傳 path?query：path 是 /o/{key}，query 包含 params、到期時間與簽章。
func (s *Signer) Presign(method, key string, params url.Values, ttl time.Duration) string {
	q := url.Values{}
	for k, v := range params {
		q[k] = append([]string(nil), v...)
	}
	q.Set(paramExpires, strconv.FormatInt(s.now().Add(ttl).Unix(), 10))
	q.Set(paramSignature, s.sign(method, key, q))
	return "/o/" + key + "?" + q.Encode()
}

func (s *Signer) Verify(method, key string, q url.Values) error {
	expires, err := strconv.ParseInt(q.Get(paramExpires), 10, 64)
	if err != nil {
		return ErrBadSignature
	}
	if !hmac.Equal([]byte(s.sign(method, key, q)), []byte(q.Get(paramSignature))) {
		return ErrBadSignature
	}
	if s.now().Unix() > expires {
		return ErrExpired
	}
	return nil
}

func (s *Signer) sign(method, key string, q url.Values) string {
	c := url.Values{}
	for k, v := range q {
		if k != paramSignature {
			c[k] = v
		}
	}
	mac := hmac.New(sha256.New, s.key)
	// url.Values.Encode 依參數名稱排序，查詢參數的順序不影響簽章。
	mac.Write([]byte(method + "\n" + key + "\n" + c.Encode()))
	return hex.EncodeToString(mac.Sum(nil))
}
