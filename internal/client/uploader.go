package client

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"video-platform/internal/domain"
)

var ErrInterrupted = errors.New("upload interrupted")

type UploadOptions struct {
	Concurrency int
	// StopAfter 模擬 client 在這次上傳了 n 個 part 之後中斷（例如關掉 App）；0 表示不中斷。
	StopAfter int
	// Resume 表示這是中斷後的續傳：還沒確認的 part 先試著用 MD5 確認，可能早就傳到 blob store 了。
	Resume bool
	// BeforePut 讓模擬器注入網路錯誤：回傳錯誤時這次 PUT 失敗、稍後重試。
	BeforePut func(part int) error
}

type UploadStats struct {
	Uploaded int   // 這次實際上傳的 part 數
	Skipped  int   // 之前已確認、這次跳過的 part
	Adopted  int   // 之前已經傳到 blob store 但還沒確認的 part：比對 MD5 後直接確認，不必重傳
	Retries  int   // 失敗後重試的次數
	Bytes    int64 // 這次實際上傳的位元組數
}

// Upload 上傳（或續傳）一支影片的內容，最後通知 server 完成。
//
// 續傳時先問 server 哪些 part 已經確認；已經傳到 blob store、只是還沒來得及確認的 part，
// 用本地算出的 MD5 請 server 確認，server 會向 blob store 比對 ETag，不必重傳。
func Upload(ctx context.Context, api *API, videoID string, data []byte, opts UploadOptions) (UploadStats, error) {
	var stats UploadStats
	status, err := api.UploadStatus(ctx, videoID)
	if err != nil {
		return stats, err
	}
	if status.Status != domain.StatusUploading {
		return stats, nil
	}
	done := make(map[int]bool, len(status.Parts))
	for _, p := range status.Parts {
		done[p.Number] = true
	}
	stats.Skipped = len(done)
	chunk := func(n int) []byte {
		start := int64(n-1) * status.PartSize
		return data[start:min(start+status.PartSize, int64(len(data)))]
	}
	var todo []int
	for n := 1; n <= status.PartCount; n++ {
		if done[n] {
			continue
		}
		if opts.Resume {
			sum := md5.Sum(chunk(n))
			if api.ConfirmPart(ctx, videoID, n, hex.EncodeToString(sum[:])) == nil {
				stats.Adopted++
				continue
			}
		}
		todo = append(todo, n)
	}

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	queue := make(chan int, len(todo))
	for _, n := range todo {
		queue <- n
	}
	close(queue)
	var mu sync.Mutex
	var uploaded atomic.Int64
	var wg sync.WaitGroup
	for range max(1, opts.Concurrency) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range queue {
				if ctx.Err() != nil {
					return
				}
				retries, err := uploadPart(ctx, api, videoID, n, chunk(n), opts.BeforePut)
				mu.Lock()
				stats.Retries += retries
				if err == nil {
					stats.Uploaded++
					stats.Bytes += int64(len(chunk(n)))
				}
				mu.Unlock()
				if err != nil {
					cancel(err)
					return
				}
				if opts.StopAfter > 0 && uploaded.Add(1) >= int64(opts.StopAfter) {
					cancel(ErrInterrupted)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := context.Cause(ctx); err != nil {
		return stats, err
	}
	_, err = api.Complete(ctx, videoID)
	return stats, err
}

// uploadPart 用 presigned URL 把一個 part 直接 PUT 到 blob store，再請 server 確認。失敗就退避重試。
func uploadPart(ctx context.Context, api *API, videoID string, n int, body []byte, beforePut func(int) error) (int, error) {
	retries := 0
	backoff := 20 * time.Millisecond
	var url string
	for {
		err := func() error {
			if url == "" {
				urls, err := api.UploadURLs(ctx, videoID, []int{n})
				if err != nil {
					return err
				}
				url = urls[n]
			}
			if beforePut != nil {
				if err := beforePut(n); err != nil {
					return err
				}
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
			if err != nil {
				return err
			}
			resp, err := api.http.Do(req)
			if err != nil {
				return err
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusForbidden {
				url = "" // 網址過期：重新要一個
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("PUT part %d: HTTP %d", n, resp.StatusCode)
			}
			return api.ConfirmPart(ctx, videoID, n, strings.Trim(resp.Header.Get("ETag"), `"`))
		}()
		if err == nil || ctx.Err() != nil {
			return retries, err
		}
		var he *HTTPError
		if errors.As(err, &he) && he.Status < 500 && he.Status != http.StatusConflict {
			return retries, err
		}
		retries++
		if retries > 8 {
			return retries, err
		}
		select {
		case <-ctx.Done():
			return retries, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, time.Second)
	}
}
