package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"time"

	"video-platform/internal/domain"
	"video-platform/internal/media"
)

type PlayerConfig struct {
	API *API
	// CDN 是使用者所在區域的 edge（實務上由 GeoDNS 決定）。
	CDN  string
	Link *Link
	// ABR 關閉時永遠下載最高畫質，作為對照組。
	ABR bool
	// MaxBuffer：緩衝超過這麼多就先不下載。
	MaxBuffer time.Duration
	// LowBuffer：緩衝低於這個值時，不管頻寬估計多少都往下切一級。
	LowBuffer time.Duration
	// Safety：只選 BANDWIDTH 不超過「估計頻寬 × Safety」的畫質，保留餘裕給頻寬波動。
	Safety float64
	// OnSegment 收到每一段解碼後的 frame，模擬器用它驗證內容。
	OnSegment func(rendition string, frames []media.EncodedFrame)
}

type Playback struct {
	VideoID   string
	Startup   time.Duration
	Stalls    int
	StallTime time.Duration
	Media     time.Duration
	Bits      int64
	Switches  int
	Segments  map[string]int
	Frames    int
}

// AvgBitrate 是實際下載內容的平均位元率。
func (p Playback) AvgBitrate() int {
	if p.Media <= 0 {
		return 0
	}
	return int(float64(p.Bits) / p.Media.Seconds())
}

// RebufferRatio 是卡住的時間佔觀看總時間的比例。
func (p Playback) RebufferRatio() float64 {
	total := p.Media + p.StallTime
	if total <= 0 {
		return 0
	}
	return p.StallTime.Seconds() / total.Seconds()
}

// playClock 追蹤播放進度：已下載的內容長度、目前播放到哪裡、卡住了多久。
type playClock struct {
	started    bool
	stalled    bool
	playhead   float64 // 秒
	buffered   float64
	last       time.Time
	stallStart time.Time
	stalls     int
	stallTime  time.Duration
}

func (c *playClock) advance(now time.Time) {
	if c.started && !c.stalled {
		c.playhead += now.Sub(c.last).Seconds()
		if c.playhead >= c.buffered {
			// 播放追上下載：從剛好用完緩衝的那一刻開始算卡住。
			over := c.playhead - c.buffered
			c.playhead = c.buffered
			c.stalled = true
			c.stalls++
			c.stallStart = now.Add(-time.Duration(over * float64(time.Second)))
		}
	}
	c.last = now
}

func (c *playClock) add(now time.Time, seconds float64) {
	c.advance(now)
	c.buffered += seconds
	c.started = true
	if c.stalled {
		c.stallTime += now.Sub(c.stallStart)
		c.stalled = false
	}
}

func (c *playClock) level() float64 { return c.buffered - c.playhead }

type player struct {
	cfg       PlayerConfig
	http      *http.Client
	base      string
	variants  []media.Variant // 由低到高
	playlists map[string][]media.MediaSegment
}

// Play 從頭到尾播放一支影片：取得 metadata 與 manifest，逐段下載並依頻寬與緩衝選擇畫質。
func Play(ctx context.Context, cfg PlayerConfig, videoID string) (Playback, error) {
	pb := Playback{VideoID: videoID, Segments: map[string]int{}}
	start := time.Now()
	info, err := cfg.API.Video(ctx, videoID)
	if err != nil {
		return pb, err
	}
	if info.Status != domain.StatusReady {
		return pb, fmt.Errorf("video %s is %s", videoID, info.Status)
	}
	p := &player{cfg: cfg, http: cfg.Link.Client(), base: path.Dir(info.Manifest), playlists: map[string][]media.MediaSegment{}}
	master, _, err := p.get(ctx, info.Manifest)
	if err != nil {
		return pb, err
	}
	if p.variants, err = media.ParseMaster(master); err != nil {
		return pb, err
	}
	slices.SortFunc(p.variants, func(a, b media.Variant) int { return a.Bandwidth - b.Bandwidth })

	// 第一段用最低畫質：先讓畫面出來，下載它的同時也量到了第一個頻寬樣本。
	cur := 0
	if !cfg.ABR {
		cur = len(p.variants) - 1
	}
	ref, err := p.playlist(ctx, cur)
	if err != nil {
		return pb, err
	}
	var clock playClock
	var fast, slow float64
	nextFrame := uint32(0)
	for i := range ref {
		clock.advance(time.Now())
		for clock.level() > cfg.MaxBuffer.Seconds() {
			wait := time.Duration((clock.level() - cfg.MaxBuffer.Seconds() + 0.05) * float64(time.Second))
			select {
			case <-ctx.Done():
				return pb, ctx.Err()
			case <-time.After(wait):
			}
			clock.advance(time.Now())
		}
		next := cur
		if cfg.ABR && i > 0 {
			next = p.choose(min(fast, slow), clock.level(), cur)
		}
		segs, err := p.playlist(ctx, next)
		if err != nil {
			return pb, err
		}
		if len(segs) != len(ref) || segs[i].Duration != ref[i].Duration {
			return pb, fmt.Errorf("%w: renditions are not aligned at segment %d", media.ErrCorrupt, i)
		}
		data, transfer, err := p.get(ctx, path.Join(p.base, path.Dir(p.variants[next].URI), segs[i].URI))
		if err != nil {
			return pb, err
		}
		sample := float64(len(data)*8) / max(transfer.Seconds(), 1e-3)
		if fast == 0 {
			fast, slow = sample, sample
		}
		// 兩個指數移動平均取小的：頻寬下降時反應快，上升時謹慎。
		fast = 0.5*sample + 0.5*fast
		slow = 0.1*sample + 0.9*slow

		name, frames, err := media.DecodeSegment(data)
		if err != nil {
			return pb, err
		}
		if len(frames) == 0 {
			return pb, fmt.Errorf("%w: segment %d of %s is empty", media.ErrCorrupt, i, name)
		}
		if name != p.variants[next].Name || !frames[0].Key || frames[0].Index != nextFrame {
			return pb, fmt.Errorf("%w: segment %d of %s starts at frame %d (keyframe %v), expected %d", media.ErrCorrupt, i, name, frames[0].Index, frames[0].Key, nextFrame)
		}
		nextFrame += uint32(len(frames))
		if cfg.OnSegment != nil {
			cfg.OnSegment(name, frames)
		}
		now := time.Now()
		clock.add(now, segs[i].Duration)
		if i == 0 {
			pb.Startup = now.Sub(start)
		}
		if i > 0 && next != cur {
			pb.Switches++
		}
		cur = next
		pb.Segments[name]++
		pb.Bits += int64(len(data) * 8)
		pb.Media += time.Duration(segs[i].Duration * float64(time.Second))
		pb.Frames += len(frames)
	}
	pb.Stalls, pb.StallTime = clock.stalls, clock.stallTime
	return pb, nil
}

// choose 選出頻寬撐得住的最高畫質；緩衝快要見底時，不管估計多少都比目前低一級。
func (p *player) choose(estimate, buffer float64, cur int) int {
	pick := 0
	for i, v := range p.variants {
		if float64(v.Bandwidth) <= estimate*p.cfg.Safety {
			pick = i
		}
	}
	if buffer < p.cfg.LowBuffer.Seconds() && pick >= cur {
		pick = max(0, cur-1)
	}
	return pick
}

func (p *player) playlist(ctx context.Context, i int) ([]media.MediaSegment, error) {
	v := p.variants[i]
	if segs, ok := p.playlists[v.Name]; ok {
		return segs, nil
	}
	data, _, err := p.get(ctx, path.Join(p.base, v.URI))
	if err != nil {
		return nil, err
	}
	segs, err := media.ParseMedia(data)
	if err != nil {
		return nil, err
	}
	p.playlists[v.Name] = segs
	return segs, nil
}

// get 下載一個物件，並回傳從收到回應標頭到讀完內容的時間。頻寬估計只用這段時間：
// 段落只有幾百 KB，若把 RTT 與 CDN 回源的等待也算進去，頻寬會被嚴重低估，畫質永遠升不上去。
func (p *player) get(ctx context.Context, path string) ([]byte, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.CDN+path, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("GET %s: HTTP %d", path, resp.StatusCode)
	}
	start := time.Now()
	data, err := io.ReadAll(resp.Body)
	return data, time.Since(start), err
}
