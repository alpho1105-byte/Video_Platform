package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"video-platform/internal/client"
	"video-platform/internal/media"
)

// 觀看者的網路：頻寬依時間變化。fluctuating 每 4 秒在 6 Mbps 與 0.6 Mbps 之間切換（例如在移動中的手機）。
var linkClasses = []struct {
	name      string
	share     int
	bandwidth func(phase time.Duration) func(time.Duration) int
}{
	{"fast 15 Mbps", 25, constant(15_000_000)},
	{"medium 3.5 Mbps", 30, constant(3_500_000)},
	{"slow 1 Mbps", 25, constant(1_000_000)},
	{"fluctuating 6 / 0.6 Mbps", 20, func(phase time.Duration) func(time.Duration) int {
		return func(t time.Duration) int {
			if ((t+phase)/(4*time.Second))%2 == 0 {
				return 6_000_000
			}
			return 600_000
		}
	}},
}

func constant(bps int) func(time.Duration) func(time.Duration) int {
	return func(time.Duration) func(time.Duration) int { return func(time.Duration) int { return bps } }
}

type view struct {
	region    *region
	class     string
	abr       bool
	video     *upload
	pb        client.Playback
	err       error
	badFrames int
}

// watch 讓觀看者在 15 秒內陸續到達，依熱門程度挑一支已經可以看的影片播放。四分之一的人用固定最高畫質的播放器當對照組。
func (s *sim) watch(ctx context.Context, n int) error {
	waitFor(time.Minute, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.ready) >= 5
	})
	fmt.Printf("watch     %4.1fs  %d videos ready, viewers start arriving\n", time.Since(s.start).Seconds(), len(s.readySnapshot()))
	var wg sync.WaitGroup
	begin := time.Now()
	for i := range n {
		at := time.Duration(s.rng.Int64N(int64(15 * time.Second)))
		v := &view{region: s.regions[s.rng.IntN(len(s.regions))], abr: i%4 != 0}
		pick := s.rng.IntN(100)
		phase := time.Duration(s.rng.Int64N(int64(8 * time.Second)))
		var bandwidth func(time.Duration) int
		for _, c := range linkClasses {
			if pick < c.share {
				v.class, bandwidth = c.name, c.bandwidth(phase)
				break
			}
			pick -= c.share
		}
		choice := s.rng.Float64()
		s.mu.Lock()
		s.views = append(s.views, v)
		s.mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Until(begin.Add(at)))
			v.video = s.choose(choice)
			next := 0
			v.pb, v.err = client.Play(ctx, client.PlayerConfig{API: client.NewAPI(s.api(), ""), CDN: v.region.url,
				Link: client.NewLink(v.region.rtt, bandwidth), ABR: v.abr, MaxBuffer: 10 * time.Second, LowBuffer: 2 * time.Second, Safety: 0.8,
				OnSegment: func(_ string, frames []media.EncodedFrame) {
					for _, f := range frames {
						if int(f.Index) != next || f.SourceDigest != v.video.digests[f.Index] {
							v.badFrames++
						}
						next++
					}
				}}, v.video.id)
			if v.err == nil && next != len(v.video.digests) {
				v.badFrames += len(v.video.digests) - next
			}
		}()
	}
	wg.Wait()
	return nil
}

func (s *sim) readySnapshot() []*upload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*upload(nil), s.ready...)
}

// choose 依熱門程度（Zipf）在目前可以看的影片中挑一支。
func (s *sim) choose(x float64) *upload {
	ready := s.readySnapshot()
	total := 0.0
	for _, u := range ready {
		total += u.popularity
	}
	x *= total
	for _, u := range ready {
		if x -= u.popularity; x <= 0 {
			return u
		}
	}
	return ready[len(ready)-1]
}
