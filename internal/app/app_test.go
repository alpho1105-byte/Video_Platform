package app

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"video-platform/internal/cdn"
	"video-platform/internal/client"
	"video-platform/internal/domain"
	"video-platform/internal/media"
	"video-platform/internal/synth"
	"video-platform/internal/workflow"
)

type rig struct {
	t   *testing.T
	ctx context.Context
	app *App
	api string
	cdn string
}

func newRig(t *testing.T) *rig {
	t.Helper()
	dir := t.TempDir()
	opts := DefaultOptions(dir, []byte("secret"), []byte("blob-key"))
	opts.Dev = true
	opts.Logger = slog.New(slog.DiscardHandler)
	opts.MinPart, opts.MaxPart = 64<<10, 256<<10
	opts.Videos.PartSize = 128 << 10
	opts.Processing.TranscodeCost = 0
	opts.Workflow = workflow.Config{LeaseTTL: time.Second, Poll: 5 * time.Millisecond, BaseBackoff: 5 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	opts.Pool = workflow.PoolConfig{Min: 2, Max: 4, PerWorker: 4, Interval: 20 * time.Millisecond, ScaleDownAfter: time.Second}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(opts, ln)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.Start(ctx)
	handler, _ := a.NewAPI()
	apiSrv := httptest.NewServer(handler)
	edge := httptest.NewServer(cdn.New(cdn.Config{Region: "tw", Capacity: 64 << 20}, a.BlobBase).Handler())
	t.Cleanup(func() {
		apiSrv.Close()
		edge.Close()
		cancel()
		time.Sleep(50 * time.Millisecond)
		a.Close()
	})
	return &rig{t: t, ctx: ctx, app: a, api: apiSrv.URL, cdn: edge.URL}
}

func (r *rig) login(user string) *client.API {
	api, err := client.Login(r.ctx, r.api, user)
	if err != nil {
		r.t.Fatal(err)
	}
	return api
}

func (r *rig) waitStatus(api *client.API, id string, want domain.Status) client.VideoInfo {
	r.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		info, err := api.Video(r.ctx, id)
		if err == nil && info.Status == want {
			return info
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("video %s is %s (%v), want %s", id, info.Status, err, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func video(seed uint64, corrupt synth.Corruption) ([]byte, [][8]byte) {
	return synth.Generate(synth.Spec{Seed: seed, Width: 1280, Height: 720, FPS: 30, Frames: 200, Bitrate: 1_800_000, GOP: 30, Corrupt: corrupt})
}

// 上傳到一半中斷、續傳、轉檔、透過 CDN 播放：每個 frame 都要依序出現，內容來自正確的原始 frame。
func TestUploadResumeProcessAndPlay(t *testing.T) {
	r := newRig(t)
	alice := r.login("alice")
	data, digests := video(1, synth.Intact)
	created, err := alice.CreateVideo(r.ctx, "holiday", "", int64(len(data)))
	if err != nil || created.PartCount < 4 {
		t.Fatalf("create = %+v, %v", created, err)
	}
	if _, err := client.Upload(r.ctx, alice, created.VideoID, data, client.UploadOptions{Concurrency: 1, StopAfter: 2}); !errors.Is(err, client.ErrInterrupted) {
		t.Fatalf("first attempt: %v", err)
	}
	if info, _ := alice.Video(r.ctx, created.VideoID); info.Status != domain.StatusUploading {
		t.Fatalf("status after interruption = %s", info.Status)
	}
	stats, err := client.Upload(r.ctx, alice, created.VideoID, data, client.UploadOptions{Concurrency: 3, Resume: true})
	if err != nil || stats.Skipped != 2 || stats.Skipped+stats.Adopted+stats.Uploaded != created.PartCount {
		t.Fatalf("resume = %+v, %v", stats, err)
	}
	info := r.waitStatus(alice, created.VideoID, domain.StatusReady)
	if len(info.Renditions) != 3 || info.Renditions[0] != "720p" || info.DurationMs != 6666 {
		t.Fatalf("ready video = %+v", info)
	}
	raw, _ := r.app.BlobAdmin.Get(r.ctx, "raw/"+created.VideoID)
	if md5.Sum(raw) != md5.Sum(data) {
		t.Fatal("the assembled upload differs from the source")
	}

	for _, abr := range []bool{true, false} {
		next := 0
		pb, err := client.Play(r.ctx, client.PlayerConfig{API: client.NewAPI(r.api, ""), CDN: r.cdn, ABR: abr,
			Link: client.NewLink(0, func(time.Duration) int { return 50_000_000 }), MaxBuffer: 10 * time.Second, LowBuffer: 4 * time.Second, Safety: 0.8,
			OnSegment: func(_ string, frames []media.EncodedFrame) {
				for _, f := range frames {
					if int(f.Index) != next || f.SourceDigest != digests[f.Index] {
						t.Errorf("frame %d (expected %d) does not match the source", f.Index, next)
					}
					next++
				}
			}}, created.VideoID)
		if err != nil || pb.Frames != 200 || next != 200 {
			t.Fatalf("abr=%v: played %+v, %v", abr, pb, err)
		}
		if !abr && (len(pb.Segments) != 1 || pb.Segments["720p"] == 0) {
			t.Fatalf("fixed player used %v", pb.Segments)
		}
	}
}

func TestCorruptUploadFailsTheVideo(t *testing.T) {
	r := newRig(t)
	bob := r.login("bob")
	data, _ := video(2, synth.NoLeadingKeyframe)
	created, _ := bob.CreateVideo(r.ctx, "broken", "", int64(len(data)))
	if _, err := client.Upload(r.ctx, bob, created.VideoID, data, client.UploadOptions{Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	info := r.waitStatus(bob, created.VideoID, domain.StatusFailed)
	if !strings.Contains(info.Error, "keyframe") {
		t.Fatalf("error = %q", info.Error)
	}
	if _, err := client.NewAPI(r.api, "").Video(r.ctx, created.VideoID); err == nil {
		t.Fatal("a failed video is visible to the public")
	}
}

func TestUploadProtocolRejectsMisuse(t *testing.T) {
	r := newRig(t)
	alice, mallory := r.login("alice"), r.login("mallory")
	data, _ := video(3, synth.Intact)
	created, _ := alice.CreateVideo(r.ctx, "mine", "", int64(len(data)))
	var he *client.HTTPError
	status := func(err error) int {
		if errors.As(err, &he) {
			return he.Status
		}
		return 0
	}

	if _, err := mallory.UploadURLs(r.ctx, created.VideoID, []int{1}); status(err) != http.StatusNotFound {
		t.Errorf("someone else's upload URLs: %v", err)
	}
	if _, err := alice.UploadURLs(r.ctx, created.VideoID, []int{created.PartCount + 1}); status(err) != http.StatusUnprocessableEntity {
		t.Errorf("part beyond the file: %v", err)
	}
	if _, err := alice.Complete(r.ctx, created.VideoID); status(err) != http.StatusConflict {
		t.Errorf("complete before uploading: %v", err)
	}
	urls, _ := alice.UploadURLs(r.ctx, created.VideoID, []int{1})
	req, _ := http.NewRequest(http.MethodPut, urls[1], bytes.NewReader(data[:128<<10]))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if err := alice.ConfirmPart(r.ctx, created.VideoID, 1, "00000000000000000000000000000000"); status(err) != http.StatusConflict {
		t.Errorf("confirming with a made-up ETag: %v", err)
	}
	if err := mallory.ConfirmPart(r.ctx, created.VideoID, 1, strings.Trim(resp.Header.Get("ETag"), `"`)); status(err) != http.StatusNotFound {
		t.Errorf("someone else confirming: %v", err)
	}
	if _, err := alice.CreateVideo(r.ctx, "too big", "", 1<<62); status(err) != http.StatusUnprocessableEntity {
		t.Errorf("oversized video: %v", err)
	}
	if _, err := client.NewAPI(r.api, "").Video(r.ctx, created.VideoID); status(err) != http.StatusNotFound {
		t.Errorf("an unfinished video is visible to the public: %v", err)
	}
}
