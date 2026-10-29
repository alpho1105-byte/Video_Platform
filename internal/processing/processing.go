// Package processing 定義影片的後處理 DAG：
//
//	split ──▶ transcode(段落 × 畫質) ──▶ playlist(每個畫質) ──▶ master ──▶ finalize
//
// split 先依 keyframe 把原始影片切段，之後每一段的每一種畫質都是獨立的任務，可以分散在所有 worker 上平行轉碼。
// 每個任務的輸出都寫到固定的 key，重做一次只會寫出一樣的內容，所以任務可以安全地重試。
package processing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"video-platform/internal/blob"
	"video-platform/internal/media"
	"video-platform/internal/store"
	"video-platform/internal/workflow"
)

type Config struct {
	SegmentTarget time.Duration
	// TranscodeCost 模擬轉碼 1 MB 原始資料（1080p）所需的 CPU 時間；畫質越低越快。
	TranscodeCost time.Duration
	MaxAttempts   int
}

func DefaultConfig() Config {
	return Config{SegmentTarget: 2 * time.Second, TranscodeCost: 20 * time.Millisecond, MaxAttempts: 5}
}

// Processor 實作各種任務。Inject 讓模擬器注入暫時性的錯誤（例如轉碼機器的 OOM）。
type Processor struct {
	cfg    Config
	blob   *blob.Client
	store  *store.Store
	Inject func(kind string) error
}

func New(cfg Config, bc *blob.Client, st *store.Store) *Processor {
	return &Processor{cfg: cfg, blob: bc, store: st}
}

func (p *Processor) Handlers() map[string]workflow.Handler {
	return map[string]workflow.Handler{
		"split":     p.split,
		"transcode": p.transcode,
		"playlist":  p.playlist,
		"master":    p.master,
		"finalize":  p.finalize,
	}
}

// Root 是一支影片 DAG 的起點：只有 split，其他節點在 split 完成後才知道要建立多少。
func (p *Processor) Root(videoID, rawKey string) []store.TaskSpec {
	return []store.TaskSpec{{ID: videoID + ":split", Kind: "split", Args: splitArgs{VideoID: videoID, RawKey: rawKey}, MaxAttempts: p.cfg.MaxAttempts}}
}

func srcKey(videoID string, i int) string { return fmt.Sprintf("work/%s/src/%05d.raw", videoID, i) }
func segName(i int) string                { return fmt.Sprintf("seg_%05d.seg", i) }
func vodPrefix(videoID string) string     { return blob.PublicPrefix + videoID }

type splitArgs struct {
	VideoID string `json:"video_id"`
	RawKey  string `json:"raw_key"`
}

type splitResult struct {
	DurationMs    int64               `json:"duration_ms"`
	FPS           uint16              `json:"fps"`
	SourceBitrate int                 `json:"source_bitrate"`
	Segments      []media.SegmentInfo `json:"segments"`
}

type transcodeArgs struct {
	VideoID       string `json:"video_id"`
	Segment       int    `json:"segment"`
	Rendition     string `json:"rendition"`
	FPS           uint16 `json:"fps"`
	SourceBitrate int    `json:"source_bitrate"`
}

type transcodeResult struct {
	Segment int   `json:"segment"`
	Frames  int   `json:"frames"`
	Bytes   int64 `json:"bytes"`
}

type playlistArgs struct {
	VideoID   string `json:"video_id"`
	Rendition string `json:"rendition"`
	FPS       uint16 `json:"fps"`
}

type playlistResult struct {
	Rendition        string `json:"rendition"`
	Bandwidth        int    `json:"bandwidth"`
	AverageBandwidth int    `json:"average_bandwidth"`
}

type videoArgs struct {
	VideoID    string `json:"video_id"`
	DurationMs int64  `json:"duration_ms"`
}

type masterResult struct {
	Manifest   string   `json:"manifest"`
	Renditions []string `json:"renditions"`
}

func (p *Processor) inject(kind string) error {
	if p.Inject == nil {
		return nil
	}
	return p.Inject(kind)
}

// split 切段並寫出每一段的原始資料，接著建立整個 DAG 的其餘部分。損毀的影片不會因為重試而變好，回傳 Permanent。
func (p *Processor) split(ctx context.Context, t store.Task) (workflow.Result, error) {
	var a splitArgs
	if err := json.Unmarshal(t.Args, &a); err != nil {
		return workflow.Result{}, workflow.Permanent(err)
	}
	if err := p.inject("split"); err != nil {
		return workflow.Result{}, err
	}
	body, err := p.blob.Open(ctx, a.RawKey)
	if err != nil {
		return workflow.Result{}, err
	}
	defer body.Close()
	r, err := media.NewRawReader(body)
	if err != nil {
		return workflow.Result{}, workflow.Permanent(err)
	}
	h := r.Header()
	var res splitResult
	var total int64
	err = media.Split(r, p.cfg.SegmentTarget, func(s media.SegmentInfo, frames []media.Frame) error {
		var buf bytes.Buffer
		if err := media.WriteRaw(&buf, media.Header{Width: h.Width, Height: h.Height, FPS: h.FPS, Frames: uint32(len(frames))}, frames); err != nil {
			return err
		}
		if _, err := p.blob.Put(ctx, srcKey(a.VideoID, s.Index), buf.Bytes()); err != nil {
			return err
		}
		res.Segments = append(res.Segments, s)
		total += s.Bytes
		return nil
	})
	if errors.Is(err, media.ErrCorrupt) {
		return workflow.Result{}, workflow.Permanent(err)
	}
	if err != nil {
		return workflow.Result{}, err
	}
	frames := 0
	for _, s := range res.Segments {
		frames += s.Frames
	}
	res.FPS = h.FPS
	res.DurationMs = int64(frames) * 1000 / int64(h.FPS)
	res.SourceBitrate = int(total * 8 * 1000 / max(res.DurationMs, 1))

	var spawn []store.TaskSpec
	var playlists []string
	for _, rd := range media.LadderFor(h) {
		var segs []string
		for _, s := range res.Segments {
			id := fmt.Sprintf("%s:transcode:%s:%05d", a.VideoID, rd.Name, s.Index)
			segs = append(segs, id)
			spawn = append(spawn, store.TaskSpec{ID: id, Kind: "transcode", MaxAttempts: p.cfg.MaxAttempts,
				Args: transcodeArgs{VideoID: a.VideoID, Segment: s.Index, Rendition: rd.Name, FPS: h.FPS, SourceBitrate: res.SourceBitrate}})
		}
		id := fmt.Sprintf("%s:playlist:%s", a.VideoID, rd.Name)
		playlists = append(playlists, id)
		spawn = append(spawn, store.TaskSpec{ID: id, Kind: "playlist", After: segs, MaxAttempts: p.cfg.MaxAttempts,
			Args: playlistArgs{VideoID: a.VideoID, Rendition: rd.Name, FPS: h.FPS}})
	}
	master := a.VideoID + ":master"
	spawn = append(spawn,
		store.TaskSpec{ID: master, Kind: "master", After: playlists, MaxAttempts: p.cfg.MaxAttempts, Args: videoArgs{VideoID: a.VideoID}},
		store.TaskSpec{ID: a.VideoID + ":finalize", Kind: "finalize", After: []string{master}, MaxAttempts: p.cfg.MaxAttempts,
			Args: videoArgs{VideoID: a.VideoID, DurationMs: res.DurationMs}})
	return workflow.Result{Output: res, Spawn: spawn}, nil
}

func (p *Processor) transcode(ctx context.Context, t store.Task) (workflow.Result, error) {
	var a transcodeArgs
	if err := json.Unmarshal(t.Args, &a); err != nil {
		return workflow.Result{}, workflow.Permanent(err)
	}
	rd, ok := media.RenditionByName(a.Rendition)
	if !ok {
		return workflow.Result{}, workflow.Permanent(fmt.Errorf("unknown rendition %q", a.Rendition))
	}
	if err := p.inject("transcode"); err != nil {
		return workflow.Result{}, err
	}
	data, err := p.blob.Get(ctx, srcKey(a.VideoID, a.Segment))
	if err != nil {
		return workflow.Result{}, err
	}
	r, err := media.NewRawReader(bytes.NewReader(data))
	if err != nil {
		return workflow.Result{}, workflow.Permanent(err)
	}
	var out []media.EncodedFrame
	for {
		f, err := r.Next()
		if err != nil {
			if errors.Is(err, media.ErrCorrupt) {
				return workflow.Result{}, workflow.Permanent(err)
			}
			break
		}
		out = append(out, media.Transcode(f, rd, a.SourceBitrate))
	}
	// 模擬轉碼的 CPU 時間：和原始資料量與輸出解析度成正比。
	cost := time.Duration(float64(p.cfg.TranscodeCost) * float64(len(data)) / (1 << 20) * (0.3 + float64(rd.Height)/1080))
	select {
	case <-ctx.Done():
		return workflow.Result{}, ctx.Err()
	case <-time.After(cost):
	}
	seg := media.EncodeSegment(rd.Name, out)
	if _, err := p.blob.Put(ctx, fmt.Sprintf("%s/%s/%s", vodPrefix(a.VideoID), rd.Name, segName(a.Segment)), seg); err != nil {
		return workflow.Result{}, err
	}
	return workflow.Result{Output: transcodeResult{Segment: a.Segment, Frames: len(out), Bytes: int64(len(seg))}}, nil
}

// playlist 依各段的轉碼結果寫出這個畫質的 media manifest，並算出峰值與平均位元率給 master manifest 用。
func (p *Processor) playlist(ctx context.Context, t store.Task) (workflow.Result, error) {
	var a playlistArgs
	if err := json.Unmarshal(t.Args, &a); err != nil {
		return workflow.Result{}, workflow.Permanent(err)
	}
	var segs []transcodeResult
	for _, raw := range t.Inputs {
		var r transcodeResult
		if err := json.Unmarshal(raw, &r); err != nil {
			return workflow.Result{}, workflow.Permanent(err)
		}
		segs = append(segs, r)
	}
	slices.SortFunc(segs, func(x, y transcodeResult) int { return x.Segment - y.Segment })
	entries := make([]media.MediaSegment, len(segs))
	peak, totalBits, totalSec := 0, 0.0, 0.0
	for i, s := range segs {
		if s.Segment != i {
			return workflow.Result{}, workflow.Permanent(fmt.Errorf("segment %d missing from %s", i, a.Rendition))
		}
		dur := float64(s.Frames) / float64(a.FPS)
		entries[i] = media.MediaSegment{URI: segName(i), Duration: dur}
		peak = max(peak, int(float64(s.Bytes*8)/dur))
		totalBits += float64(s.Bytes * 8)
		totalSec += dur
	}
	key := fmt.Sprintf("%s/%s/index.m3u8", vodPrefix(a.VideoID), a.Rendition)
	if _, err := p.blob.Put(ctx, key, media.WriteMedia(entries)); err != nil {
		return workflow.Result{}, err
	}
	return workflow.Result{Output: playlistResult{Rendition: a.Rendition, Bandwidth: peak, AverageBandwidth: int(totalBits / totalSec)}}, nil
}

func (p *Processor) master(ctx context.Context, t store.Task) (workflow.Result, error) {
	var a videoArgs
	if err := json.Unmarshal(t.Args, &a); err != nil {
		return workflow.Result{}, workflow.Permanent(err)
	}
	var variants []media.Variant
	for _, raw := range t.Inputs {
		var r playlistResult
		if err := json.Unmarshal(raw, &r); err != nil {
			return workflow.Result{}, workflow.Permanent(err)
		}
		rd, _ := media.RenditionByName(r.Rendition)
		variants = append(variants, media.Variant{URI: r.Rendition + "/index.m3u8", Bandwidth: r.Bandwidth, AverageBandwidth: r.AverageBandwidth,
			Width: rd.Width, Height: rd.Height, Name: r.Rendition})
	}
	slices.SortFunc(variants, func(x, y media.Variant) int { return y.Bandwidth - x.Bandwidth })
	key := vodPrefix(a.VideoID) + "/master.m3u8"
	if _, err := p.blob.Put(ctx, key, media.WriteMaster(variants)); err != nil {
		return workflow.Result{}, err
	}
	res := masterResult{Manifest: key}
	for _, v := range variants {
		res.Renditions = append(res.Renditions, v.Name)
	}
	return workflow.Result{Output: res}, nil
}

// finalize 把影片標成 ready（之後才會出現在觀看的 API 裡），再清掉切段的中間檔。
func (p *Processor) finalize(ctx context.Context, t store.Task) (workflow.Result, error) {
	var a videoArgs
	if err := json.Unmarshal(t.Args, &a); err != nil {
		return workflow.Result{}, workflow.Permanent(err)
	}
	var m masterResult
	for _, raw := range t.Inputs {
		if err := json.Unmarshal(raw, &m); err != nil {
			return workflow.Result{}, workflow.Permanent(err)
		}
	}
	if err := p.store.MarkReady(ctx, a.VideoID, a.DurationMs, m.Renditions, m.Manifest); err != nil {
		return workflow.Result{}, err
	}
	if _, err := p.blob.DeletePrefix(ctx, "work/"+a.VideoID); err != nil && !errors.Is(err, blob.ErrNotFound) {
		return workflow.Result{}, err
	}
	return workflow.Result{}, nil
}
