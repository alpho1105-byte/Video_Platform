// simulate 在本機啟動完整的影音平台（Video Service、blob store、轉檔 worker pool、三個區域的 CDN edge），
// 讓上傳者以 multipart 直接上傳到 blob store（途中斷線、中斷後續傳），轉檔途中讓 worker 當機或停住，
// 再讓幾百個觀看者在不同的網路條件下用自適應位元率播放，最後驗證每個 frame 都正確送達。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"

	"video-platform/internal/app"
	"video-platform/internal/cdn"
	"video-platform/internal/synth"
	"video-platform/internal/videos"
	"video-platform/internal/workflow"
)

const (
	partSize = 512 << 10
	leaseTTL = 600 * time.Millisecond
)

func main() {
	seed := flag.Uint64("seed", 42, "random seed")
	nVideos := flag.Int("videos", 30, "number of valid videos to upload (two corrupt files are added)")
	nViewers := flag.Int("viewers", 300, "number of viewers")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := run(*seed, *nVideos, *nViewers, logger); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	if failures > 0 {
		fmt.Printf("\n%d check(s) failed\n", failures)
		os.Exit(1)
	}
}

func simOptions(dir string, logger *slog.Logger) app.Options {
	opts := app.DefaultOptions(dir, []byte("sim-secret"), []byte("sim-blob-key"))
	opts.Dev = true
	opts.Logger = logger
	opts.MinPart, opts.MaxPart = 256<<10, 1<<20
	opts.Videos = videos.Config{PartSize: partSize, MaxSize: 1 << 30, URLTTL: time.Minute}
	opts.Processing.TranscodeCost = 80 * time.Millisecond
	opts.Workflow = workflow.Config{LeaseTTL: leaseTTL, Poll: 10 * time.Millisecond, BaseBackoff: 20 * time.Millisecond, MaxBackoff: 300 * time.Millisecond}
	opts.Pool = workflow.PoolConfig{Min: 2, Max: 12, PerWorker: 6, Interval: 100 * time.Millisecond, ScaleDownAfter: time.Second}
	return opts
}

type region struct {
	name   string
	origin time.Duration
	rtt    time.Duration
	edge   *cdn.Edge
	url    string
}

type sim struct {
	app     *app.App
	apis    []string
	caches  []*videos.Cache
	regions []*region
	rng     *rand.Rand
	start   time.Time

	mu      sync.Mutex
	uploads []*upload
	ready   []*upload
	views   []*view
	probes  []probe
}

func run(seed uint64, nVideos, nViewers int, logger *slog.Logger) error {
	dir, err := os.MkdirTemp("", "video-sim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	a, err := app.New(simOptions(filepath.Join(dir, "data"), logger), ln)
	if err != nil {
		return err
	}
	defer a.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Start(ctx)

	s := &sim{app: a, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	// 兩個 Video Service 實例放在「負載平衡器」後面：client 每個請求隨機挑一個。
	for range 2 {
		h, cache := a.NewAPI()
		srv := httptest.NewServer(h)
		defer srv.Close()
		s.apis = append(s.apis, srv.URL)
		s.caches = append(s.caches, cache)
	}
	for _, r := range []*region{{name: "tw", origin: 10 * time.Millisecond, rtt: 10 * time.Millisecond},
		{name: "us", origin: 60 * time.Millisecond, rtt: 15 * time.Millisecond},
		{name: "eu", origin: 120 * time.Millisecond, rtt: 20 * time.Millisecond}} {
		r.edge = cdn.New(cdn.Config{Region: r.name, Capacity: 128 << 20, OriginLatency: r.origin}, a.BlobBase)
		srv := httptest.NewServer(r.edge.Handler())
		defer srv.Close()
		r.url = srv.URL
		s.regions = append(s.regions, r)
	}

	s.uploads = s.plan(nVideos)
	fmt.Printf("setup     %d uploads: %d videos of 6-16s at 1080p or 720p, plus %d corrupt files; parts of %d KiB\n",
		len(s.uploads), nVideos, len(s.uploads)-nVideos, partSize>>10)
	fmt.Printf("          2 Video Service instances, transcoding pool of 2-12 workers, CDN edges in tw / us / eu (origin RTT 10 / 60 / 120ms)\n\n")

	s.start = time.Now()
	s.runProbes(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	errs := make(chan error, 2)
	go func() {
		defer wg.Done()
		if err := s.uploadAll(ctx); err != nil {
			errs <- err
		}
	}()
	go func() {
		defer wg.Done()
		s.faults(ctx)
	}()
	viewersDone := make(chan error, 1)
	go func() { viewersDone <- s.watch(ctx, nViewers) }()
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	if err := <-viewersDone; err != nil {
		return err
	}
	// 等 autoscaler 在 backlog 清空後把 worker 縮回最小值。
	waitFor(5*time.Second, func() bool { cur, _ := a.Pool.Size(); return cur == 2 })
	fmt.Printf("done      %.1fs after the first upload\n\n", time.Since(s.start).Seconds())
	s.report(ctx)
	s.verify(ctx)
	return nil
}

func (s *sim) api() string { return s.apis[rand.IntN(len(s.apis))] }

// faults 在轉檔進行中製造故障：worker 當機（任務做到一半消失）、worker 停住超過租約，以及暫時性的轉檔錯誤。
func (s *sim) faults(ctx context.Context) {
	s.app.Processor.Inject = func(kind string) error {
		if (kind == "transcode" && rand.IntN(100) < 4) || (kind == "split" && rand.IntN(100) < 10) {
			return errors.New("injected: transcoder ran out of memory")
		}
		return nil
	}
	for _, f := range []struct {
		at   time.Duration
		what string
		do   func() bool
	}{
		{1500 * time.Millisecond, "a busy worker crashed", s.app.Pool.Crash},
		{2000 * time.Millisecond, "a busy worker froze for 1.5s, longer than its 600ms lease", func() bool { return s.app.Pool.Stall(1500 * time.Millisecond) }},
		{2500 * time.Millisecond, "a busy worker crashed", s.app.Pool.Crash},
		{3000 * time.Millisecond, "a busy worker froze for 1.5s, longer than its 600ms lease", func() bool { return s.app.Pool.Stall(1500 * time.Millisecond) }},
	} {
		time.Sleep(time.Until(s.start.Add(f.at)))
		for !f.do() && ctx.Err() == nil {
			time.Sleep(20 * time.Millisecond)
		}
		fmt.Printf("fault     %4.1fs  %s\n", time.Since(s.start).Seconds(), f.what)
	}
}

func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func specFor(r *rand.Rand, corrupt synth.Corruption) synth.Spec {
	sp := synth.Spec{Seed: r.Uint64(), Width: 1920, Height: 1080, FPS: 30, Frames: 30 * (6 + r.IntN(11)), Bitrate: 3_000_000,
		GOP: []int{30, 45, 60, 0}[r.IntN(4)], Corrupt: corrupt}
	if r.IntN(10) < 3 {
		sp.Width, sp.Height, sp.Bitrate = 1280, 720, 1_800_000
	}
	return sp
}
