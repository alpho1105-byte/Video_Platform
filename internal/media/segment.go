package media

import (
	"fmt"
	"io"
	"time"
)

// SegmentInfo 描述切出來的一段。所有 rendition 都由同一份切段結果轉碼，段落邊界因此完全一致，
// 播放器才能在任何一段的開頭切換畫質。
type SegmentInfo struct {
	Index      int    `json:"index"`
	FirstFrame uint32 `json:"first_frame"`
	Frames     int    `json:"frames"`
	Bytes      int64  `json:"bytes"`
}

func (s SegmentInfo) Duration(fps uint16) time.Duration {
	return time.Duration(s.Frames) * time.Second / time.Duration(fps)
}

// Split 依 keyframe 切段：一段累積到 target 之後，在下一個 keyframe 開始新的一段。
// 每一段都從 keyframe 開始，才能獨立解碼。emit 依序收到每一段的資訊與 frame。
func Split(r *RawReader, target time.Duration, emit func(SegmentInfo, []Frame) error) error {
	fps := r.Header().FPS
	targetFrames := int(target * time.Duration(fps) / time.Second)
	var cur []Frame
	var bytes int64
	index := 0
	flush := func() error {
		info := SegmentInfo{Index: index, FirstFrame: cur[0].Index, Frames: len(cur), Bytes: bytes}
		if err := emit(info, cur); err != nil {
			return err
		}
		index++
		cur, bytes = nil, 0
		return nil
	}
	for {
		f, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(cur) == 0 && !f.Key {
			return fmt.Errorf("%w: frame %d starts a segment but is not a keyframe", ErrCorrupt, f.Index)
		}
		if f.Key && len(cur) >= targetFrames {
			if err := flush(); err != nil {
				return err
			}
		}
		cur = append(cur, f)
		bytes += int64(len(f.Payload))
	}
	if len(cur) == 0 {
		return fmt.Errorf("%w: no frames", ErrCorrupt)
	}
	return flush()
}
