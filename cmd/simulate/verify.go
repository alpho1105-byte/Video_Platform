package main

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"video-platform/internal/blob"
	"video-platform/internal/domain"
	"video-platform/internal/media"
	"video-platform/internal/metrics"
	"video-platform/internal/synth"
)

var failures int

func check(desc string, ok bool) {
	mark := "ok  "
	if !ok {
		mark = "FAIL"
		failures++
	}
	fmt.Printf("  [%s] %s\n", mark, desc)
}

func ms(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%.0fms", float64(d.Microseconds())/1000)
}

func mbps(bps float64) string { return fmt.Sprintf("%.2f Mbps", bps/1e6) }

type group struct {
	n                int
	bits             float64
	media, stall     time.Duration
	switches, stalls int
	top              int
	segments         int
}

func (g *group) add(v *view) {
	g.n++
	g.bits += float64(v.pb.Bits)
	g.media += v.pb.Media
	g.stall += v.pb.StallTime
	g.switches += v.pb.Switches
	g.stalls += v.pb.Stalls
	top := media.LadderFor(media.Header{Height: v.video.spec.Height})[0].Name
	g.top += v.pb.Segments[top]
	for _, c := range v.pb.Segments {
		g.segments += c
	}
}

func (g *group) bitrate() float64 { return g.bits / math.Max(g.media.Seconds(), 1e-9) }
func (g *group) rebuffer() float64 {
	return g.stall.Seconds() / math.Max((g.media+g.stall).Seconds(), 1e-9)
}

func (s *sim) groups() map[string][2]*group {
	out := map[string][2]*group{}
	for _, c := range linkClasses {
		out[c.name] = [2]*group{{}, {}}
	}
	for _, v := range s.views {
		if v.err != nil {
			continue
		}
		g := out[v.class]
		if v.abr {
			g[0].add(v)
		} else {
			g[1].add(v)
		}
	}
	return out
}

func (s *sim) report(ctx context.Context) {
	var parts, skipped, adopted, retries, interrupted int
	var bytes int64
	for _, u := range s.uploads {
		parts += u.stats.Uploaded
		skipped += u.stats.Skipped
		adopted += u.stats.Adopted
		retries += u.stats.Retries
		bytes += u.stats.Bytes
		if u.interrupted {
			interrupted++
		}
	}
	fmt.Printf("upload    %d parts (%.0f MiB) went straight to the blob store with presigned URLs; %d PUTs retried after %d injected network errors\n",
		parts, float64(bytes)/(1<<20), retries, networkErrors.Load())
	fmt.Printf("          %d uploads were interrupted and resumed: %d confirmed parts skipped, %d uploaded-but-unconfirmed parts adopted by ETag\n",
		interrupted, skipped, adopted)

	tasks, _ := s.app.Store.Tasks(ctx)
	kinds := map[string]int{}
	for _, t := range tasks {
		if t.Status == "done" {
			kinds[t.Kind]++
		}
	}
	cur, peak := s.app.Pool.Size()
	es := s.app.Engine.Stats()
	fmt.Printf("process   tasks done: %d split, %d transcode, %d playlist, %d master, %d finalize; workers 2 -> peak %d -> %d\n",
		kinds["split"], kinds["transcode"], kinds["playlist"], kinds["master"], kinds["finalize"], peak, cur)
	fmt.Printf("          %d tasks abandoned by crashed workers, %d late results rejected, %d retries after injected errors, %d workflows failed\n",
		es.Abandoned.Load(), es.Rejected.Load(), es.Retried.Load(), es.Failed.Load())
	var toReady metrics.Latency
	for _, u := range s.uploads {
		if u.status == domain.StatusReady {
			toReady.Observe(u.settled.Sub(u.completed))
		}
	}
	sum := toReady.Summary()
	fmt.Printf("          upload complete -> ready: p50 %s  p95 %s  max %s\n\n", ms(sum.P50), ms(sum.P95), ms(sum.Max))

	byRegion := map[string]*metrics.Latency{}
	var fixedStartup metrics.Latency
	abrN := 0
	for _, v := range s.views {
		if v.err != nil {
			continue
		}
		if !v.abr {
			fixedStartup.Observe(v.pb.Startup)
			continue
		}
		abrN++
		if byRegion[v.region.name] == nil {
			byRegion[v.region.name] = &metrics.Latency{}
		}
		byRegion[v.region.name].Observe(v.pb.Startup)
	}
	fmt.Printf("playback  %d viewers (%d adaptive, %d always-highest); time to the first frame for adaptive players:\n", len(s.views), abrN, len(s.views)-abrN)
	for _, r := range s.regions {
		if l := byRegion[r.name]; l != nil {
			sum := l.Summary()
			fmt.Printf("          %-3s  p50 %s  p95 %s  (%d viewers)\n", r.name, ms(sum.P50), ms(sum.P95), sum.Count)
		}
	}
	fs := fixedStartup.Summary()
	fmt.Printf("          always-highest players start with the largest segment: p50 %s  p95 %s\n", ms(fs.P50), ms(fs.P95))
	fmt.Printf("\n          %-26s %-38s %s\n", "link", "adaptive bitrate", "always highest")
	gs := s.groups()
	for _, c := range linkClasses {
		a, f := gs[c.name][0], gs[c.name][1]
		fmt.Printf("          %-26s %3d viewers %s, rebuffer %4.1f%%   %3d viewers %s, rebuffer %4.1f%%\n",
			c.name, a.n, mbps(a.bitrate()), 100*a.rebuffer(), f.n, mbps(f.bitrate()), 100*f.rebuffer())
	}
	fmt.Println()
	for _, r := range s.regions {
		st := r.edge.Stats()
		reqs := st.Hits.Load() + st.Misses.Load()
		fmt.Printf("cdn %-5s %5d requests, hit ratio %.1f%%, %d origin fetches, %d coalesced misses, %d evictions, %.0f MiB served\n",
			r.name, reqs, 100*float64(st.Hits.Load())/float64(max(reqs, 1)), st.OriginFetches.Load(), st.Coalesced.Load(),
			st.Evictions.Load(), float64(st.BytesServed.Load())/(1<<20))
	}
	var hits int64
	for _, c := range s.caches {
		h, _ := c.Stats()
		hits += h
	}
	fmt.Printf("metadata  %d of %d viewer lookups were served from the Video Service instances' caches\n\n", hits, len(s.views))
}

func (s *sim) verify(ctx context.Context) {
	fmt.Println("checks")
	ok := true
	for _, u := range s.uploads {
		raw, err := s.app.BlobAdmin.Get(ctx, "raw/"+u.id)
		ok = ok && err == nil && md5.Sum(raw) == u.md5
	}
	check(fmt.Sprintf("all %d uploads were assembled byte-for-byte, including the interrupted ones", len(s.uploads)), ok)

	adopted := 0
	var odd []string
	for _, u := range s.uploads {
		count := int((u.size + partSize - 1) / partSize)
		st := u.stats
		if st.Skipped+st.Adopted+st.Uploaded != count || (u.interrupted && (st.Skipped == 0 || st.Adopted < 2)) {
			odd = append(odd, fmt.Sprintf("%s: %d parts, skipped %d, adopted %d, uploaded %d", u.id, count, st.Skipped, st.Adopted, st.Uploaded))
		}
		adopted += st.Adopted
	}
	check(fmt.Sprintf("resumed uploads re-sent no confirmed part and adopted the %d parts that had reached the blob store unconfirmed %v", adopted, odd), len(odd) == 0)

	ok = true
	var bad []string
	for _, p := range s.probes {
		if p.got != p.want {
			ok = false
			bad = append(bad, fmt.Sprintf("%s: %d", p.what, p.got))
		}
	}
	check(fmt.Sprintf("all %d misuse probes were rejected with the expected status %v", len(s.probes), bad), ok)

	ready, failedOK := 0, true
	for _, u := range s.uploads {
		switch u.spec.Corrupt {
		case synth.Intact:
			if u.status == domain.StatusReady {
				ready++
			}
		case synth.NoLeadingKeyframe:
			failedOK = failedOK && u.status == domain.StatusFailed && strings.Contains(u.reason, "keyframe")
		case synth.Truncated:
			failedOK = failedOK && u.status == domain.StatusFailed && strings.Contains(u.reason, "truncated")
		}
	}
	tasks, _ := s.app.Store.Tasks(ctx)
	pending := 0
	for _, t := range tasks {
		if t.Status == "waiting" || t.Status == "ready" || t.Status == "running" || (t.Status == "done" && t.DepsLeft != 0) {
			pending++
		}
	}
	leftovers := 0
	for _, u := range s.uploads {
		if u.status == domain.StatusReady {
			if _, err := s.app.BlobAdmin.Get(ctx, fmt.Sprintf("work/%s/src/00000.raw", u.id)); !errors.Is(err, blob.ErrNotFound) {
				leftovers++
			}
		}
	}
	check(fmt.Sprintf("%d valid videos are ready, both corrupt files failed with a reason, no task is stuck (%d) and no intermediate files remain (%d)",
		ready, pending, leftovers), ready == len(s.uploads)-2 && failedOK && pending == 0 && leftovers == 0)

	segments, problems := s.verifyOutputs(ctx)
	check(fmt.Sprintf("%d transcoded segments: every rendition is aligned on the same keyframes and carries every source frame exactly once %v",
		segments, problems), len(problems) == 0)

	es := s.app.Engine.Stats()
	check(fmt.Sprintf("crashed and frozen workers lost no work: %d abandoned tasks re-ran, %d late results were rejected, only the 2 corrupt workflows failed",
		es.Abandoned.Load(), es.Rejected.Load()), es.Abandoned.Load() > 0 && es.Rejected.Load() > 0 && es.Failed.Load() == 2 && pending == 0)

	cur, peak := s.app.Pool.Size()
	check(fmt.Sprintf("the worker pool scaled up to %d under the upload burst and back to %d afterwards", peak, cur), peak > 2 && cur == 2)

	ok = true
	frames := 0
	for _, v := range s.views {
		ok = ok && v.err == nil && v.badFrames == 0
		frames += v.pb.Frames
	}
	check(fmt.Sprintf("all %d viewers received all %d frames in order, each from the right source frame", len(s.views), frames), ok)

	gs := s.groups()
	var abr, fixed group
	for _, name := range []string{"slow 1 Mbps", "fluctuating 6 / 0.6 Mbps"} {
		for i, dst := range []*group{&abr, &fixed} {
			g := gs[name][i]
			dst.n += g.n
			dst.media += g.media
			dst.stall += g.stall
		}
	}
	check(fmt.Sprintf("on slow and fluctuating links adaptive playback rebuffered %.1f%% of the time vs %.1f%% for always-highest",
		100*abr.rebuffer(), 100*fixed.rebuffer()), abr.rebuffer() < 0.02 && abr.rebuffer() < fixed.rebuffer()/5)
	// 自適應播放的第一段固定用最低畫質讓畫面先出來，之後頻寬夠就應該一直在最高畫質。
	fast := gs["fast 15 Mbps"][0]
	topShare := float64(fast.top) / float64(max(fast.segments-fast.n, 1))
	check(fmt.Sprintf("on fast links adaptive playback switched to the top rendition after the first segment: %.0f%% of the remaining segments", 100*topShare),
		topShare >= 0.95)

	hot, hotRatio := s.hotHitRatio(5)
	var dup, evictions int64
	for _, r := range s.regions {
		evictions += r.edge.Stats().Evictions.Load()
		for _, n := range r.edge.OriginFetches() {
			dup += int64(n - 1)
		}
	}
	check(fmt.Sprintf("the 5 most-watched videos (%d views) were served %.1f%% from edge caches; an edge re-fetched an object from origin only after evicting it (%d re-fetches, %d evictions)",
		hot, 100*hotRatio, dup, evictions), hotRatio >= 0.8 && dup <= evictions)
}

// hotHitRatio 計算觀看次數最多的 n 支影片在所有 edge 上的命中率：請求數由播放器端統計，未命中數是 edge 向 origin 拉取的次數。
func (s *sim) hotHitRatio(n int) (int, float64) {
	views := map[string]int{}
	requests := map[string]int{}
	for _, v := range s.views {
		if v.err != nil {
			continue
		}
		views[v.video.id]++
		requests[v.video.id] += 1 + len(v.pb.Segments) // master manifest 與用到的每個 media manifest
		for _, c := range v.pb.Segments {
			requests[v.video.id] += c
		}
	}
	ids := make([]string, 0, len(views))
	for id := range views {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b string) int { return views[b] - views[a] })
	ids = ids[:min(n, len(ids))]
	total, reqs, fetches := 0, 0, 0
	for _, id := range ids {
		total += views[id]
		reqs += requests[id]
		for _, r := range s.regions {
			for key, c := range r.edge.OriginFetches() {
				if strings.HasPrefix(key, "vod/"+id+"/") {
					fetches += c
				}
			}
		}
	}
	return total, 1 - float64(fetches)/float64(max(reqs, 1))
}

// verifyOutputs 直接從 blob store 讀出每支 ready 影片的 manifest 與所有段落，逐一檢查。
func (s *sim) verifyOutputs(ctx context.Context) (int, []string) {
	var problems []string
	segments := 0
	for _, u := range s.uploads {
		if u.status != domain.StatusReady {
			continue
		}
		fail := func(format string, args ...any) {
			problems = append(problems, u.id+": "+fmt.Sprintf(format, args...))
		}
		base := "vod/" + u.id
		data, err := s.app.BlobAdmin.Get(ctx, base+"/master.m3u8")
		if err != nil {
			fail("master: %v", err)
			continue
		}
		variants, err := media.ParseMaster(data)
		if err != nil {
			fail("master: %v", err)
			continue
		}
		var want []string
		for _, r := range media.LadderFor(media.Header{Height: u.spec.Height}) {
			want = append(want, r.Name)
		}
		var got []string
		for _, v := range variants {
			got = append(got, v.Name)
			if v.Bandwidth < v.AverageBandwidth {
				fail("%s: BANDWIDTH below AVERAGE-BANDWIDTH", v.Name)
			}
		}
		if !slices.Equal(got, want) {
			fail("renditions %v, want %v", got, want)
		}
		type boundary struct {
			first uint32
			n     int
		}
		var ref []boundary
		for vi, v := range variants {
			pl, err := s.app.BlobAdmin.Get(ctx, base+"/"+v.URI)
			if err != nil {
				fail("%s playlist: %v", v.Name, err)
				continue
			}
			segs, err := media.ParseMedia(pl)
			if err != nil {
				fail("%s playlist: %v", v.Name, err)
				continue
			}
			next, total := uint32(0), 0.0
			var bounds []boundary
			for _, sg := range segs {
				raw, err := s.app.BlobAdmin.Get(ctx, base+"/"+v.Name+"/"+sg.URI)
				if err != nil {
					fail("%s: %v", sg.URI, err)
					break
				}
				name, frames, err := media.DecodeSegment(raw)
				if err != nil || name != v.Name || len(frames) == 0 || !frames[0].Key || frames[0].Index != next {
					fail("%s/%s does not continue at frame %d as a keyframe (%v)", v.Name, sg.URI, next, err)
					break
				}
				for _, f := range frames {
					if f.SourceDigest != u.digests[f.Index] {
						fail("%s frame %d has the wrong content", v.Name, f.Index)
					}
				}
				if math.Abs(sg.Duration-float64(len(frames))/float64(u.spec.FPS)) > 0.001 {
					fail("%s EXTINF %.3f for %d frames", sg.URI, sg.Duration, len(frames))
				}
				bounds = append(bounds, boundary{frames[0].Index, len(frames)})
				next += uint32(len(frames))
				total += sg.Duration
				segments++
			}
			if int(next) != len(u.digests) {
				fail("%s covers %d of %d frames", v.Name, next, len(u.digests))
			}
			if vi == 0 {
				ref = bounds
			} else if !slices.Equal(ref, bounds) {
				fail("%s segment boundaries differ from %s", v.Name, variants[0].Name)
			}
		}
	}
	return segments, problems
}
