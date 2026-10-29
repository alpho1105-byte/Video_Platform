package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"video-platform/internal/client"
	"video-platform/internal/domain"
	"video-platform/internal/synth"
)

type upload struct {
	spec        synth.Spec
	interrupted bool
	digests     [][8]byte
	md5         [16]byte
	size        int64
	popularity  float64

	id        string
	stats     client.UploadStats
	completed time.Time
	settled   time.Time
	status    domain.Status
	reason    string
}

func (s *sim) plan(n int) []*upload {
	var out []*upload
	for i := range n {
		u := &upload{spec: specFor(s.rng, synth.Intact), interrupted: i%4 == 1}
		out = append(out, u)
	}
	out = append(out, &upload{spec: specFor(s.rng, synth.NoLeadingKeyframe)}, &upload{spec: specFor(s.rng, synth.Truncated)})
	// 熱門程度服從 Zipf 分布：少數影片吸走大部分的觀看。
	for rank, i := range s.rng.Perm(n) {
		out[i].popularity = 1 / math.Pow(float64(rank+1), 1.2)
	}
	return out
}

var networkErrors atomic.Int64

// uploadAll 同時讓最多 8 個上傳者上傳。每個 PUT 有 3% 的機率遇到網路錯誤；每 4 支影片有一支會在中途中斷，
// 而且中斷時有兩個 part 已經傳到 blob store、還沒來得及向 server 確認，之後再續傳。
func (s *sim) uploadAll(ctx context.Context) error {
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	errs := make(chan error, len(s.uploads))
	for i, u := range s.uploads {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.uploadOne(ctx, i, u); err != nil {
				errs <- fmt.Errorf("upload %d: %w", i, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func (s *sim) uploadOne(ctx context.Context, i int, u *upload) error {
	data, digests := synth.Generate(u.spec)
	u.digests, u.md5, u.size = digests, md5.Sum(data), int64(len(data))
	api, err := client.Login(ctx, s.api(), fmt.Sprintf("uploader-%d", i))
	if err != nil {
		return err
	}
	created, err := api.CreateVideo(ctx, fmt.Sprintf("video %d", i), "", int64(len(data)))
	if err != nil {
		return err
	}
	u.id = created.VideoID
	flaky := func(int) error {
		if rand.IntN(100) < 3 {
			networkErrors.Add(1)
			return errors.New("injected: connection reset")
		}
		return nil
	}
	opts := client.UploadOptions{Concurrency: 4, BeforePut: flaky}
	if u.interrupted {
		first := opts
		first.StopAfter = created.PartCount * 2 / 5
		st, err := client.Upload(ctx, api, u.id, data, first)
		if !errors.Is(err, client.ErrInterrupted) {
			return fmt.Errorf("expected an interruption, got %v", err)
		}
		u.stats.Retries += st.Retries
		if err := s.uploadUnconfirmed(ctx, api, u.id, data, 2); err != nil {
			return err
		}
		time.Sleep(300 * time.Millisecond)
		opts.Resume = true
	}
	st, err := client.Upload(ctx, api, u.id, data, opts)
	if err != nil {
		return err
	}
	u.stats.Uploaded, u.stats.Skipped, u.stats.Adopted, u.stats.Bytes = st.Uploaded, st.Skipped, st.Adopted, st.Bytes
	u.stats.Retries += st.Retries
	u.completed = time.Now()
	// 上傳者輪詢狀態直到處理完成。
	for ctx.Err() == nil {
		info, err := api.Video(ctx, u.id)
		if err == nil && info.Status.Final() {
			u.settled, u.status, u.reason = time.Now(), info.Status, info.Error
			if info.Status == domain.StatusReady {
				s.mu.Lock()
				s.ready = append(s.ready, u)
				s.mu.Unlock()
			}
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ctx.Err()
}

// uploadUnconfirmed 模擬 App 在 PUT 成功之後、向 server 確認之前就被關掉：part 已經在 blob store，server 卻不知道。
func (s *sim) uploadUnconfirmed(ctx context.Context, api *client.API, id string, data []byte, n int) error {
	st, err := api.UploadStatus(ctx, id)
	if err != nil {
		return err
	}
	have := map[int]bool{}
	for _, p := range st.Parts {
		have[p.Number] = true
	}
	for part := 1; part <= st.PartCount && n > 0; part++ {
		if have[part] {
			continue
		}
		urls, err := api.UploadURLs(ctx, id, []int{part})
		if err != nil {
			return err
		}
		start := int64(part-1) * st.PartSize
		body := data[start:min(start+st.PartSize, int64(len(data)))]
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, urls[part], bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		msg, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("PUT part %d: HTTP %d: %s", part, resp.StatusCode, msg)
		}
		n--
	}
	return nil
}

type probe struct {
	what string
	want int
	got  int
}

// runProbes 嘗試各種不該成功的操作，記錄得到的 HTTP 狀態碼。
func (s *sim) runProbes(ctx context.Context) {
	owner, _ := client.Login(ctx, s.api(), "probe-owner")
	other, _ := client.Login(ctx, s.api(), "probe-other")
	data := bytes.Repeat([]byte("x"), 3*partSize)
	created, _ := owner.CreateVideo(ctx, "probe", "", int64(len(data)))
	status := func(err error) int {
		var he *client.HTTPError
		if errors.As(err, &he) {
			return he.Status
		}
		if err == nil {
			return 200
		}
		return -1
	}
	put := func(url string, body []byte) (int, string) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return -1, ""
		}
		resp.Body.Close()
		return resp.StatusCode, strings.Trim(resp.Header.Get("ETag"), `"`)
	}
	urls, _ := owner.UploadURLs(ctx, created.VideoID, []int{1, 2})
	_, etag1 := put(urls[1], data[:partSize])
	_, err := other.UploadURLs(ctx, created.VideoID, []int{1})
	add := func(what string, want, got int) { s.probes = append(s.probes, probe{what, want, got}) }
	add("upload URLs for someone else's video", 404, status(err))
	tampered, _ := put(strings.Replace(urls[2], "partNumber=2", "partNumber=3", 1), data[:partSize])
	add("presigned URL edited to another part", 403, tampered)
	signed := strings.Replace(urls[2], "raw/"+created.VideoID, "raw/v_00000000deadbeef", 1)
	moved, _ := put(signed, data[:partSize])
	add("presigned URL edited to another object", 403, moved)
	big, _ := put(urls[2], bytes.Repeat([]byte("y"), (1<<20)+1))
	add("part larger than the 1 MiB limit", 413, big)
	add("confirming a part with a made-up ETag", 409, status(owner.ConfirmPart(ctx, created.VideoID, 2, "0123456789abcdef0123456789abcdef")))
	add("someone else confirming a part", 404, status(other.ConfirmPart(ctx, created.VideoID, 1, etag1)))
	_, err = owner.Complete(ctx, created.VideoID)
	add("completing before every part is uploaded", 409, status(err))
	_, err = client.NewAPI(s.api(), "").Video(ctx, created.VideoID)
	add("public read of an unfinished video", 404, status(err))
}
