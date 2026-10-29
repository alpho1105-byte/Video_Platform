// Package synth 產生測試與模擬用的原始影片：固定 seed 得到固定內容，並回傳每個 frame 的指紋供驗證。
package synth

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"

	"video-platform/internal/media"
)

type Corruption int

const (
	Intact Corruption = iota
	// NoLeadingKeyframe：第一個 frame 不是 keyframe，任何播放器都無法從頭解碼。
	NoLeadingKeyframe
	// Truncated：檔案在中途被截斷（例如上傳的是還沒錄完的檔案）。
	Truncated
)

type Spec struct {
	Seed    uint64
	Width   uint16
	Height  uint16
	FPS     uint16
	Frames  int
	Bitrate int
	// GOP 是 keyframe 的間隔（frame 數）；0 表示每個 GOP 隨機 20 到 70 個 frame。
	GOP     int
	Corrupt Corruption
}

// Generate 回傳原始影片的內容與每個 frame payload 的指紋。
// 每個 GOP 有自己的畫面複雜度（0.5 到 1.8 倍），keyframe 是一般 frame 的 3 倍大，所以位元率會起伏。
func Generate(s Spec) ([]byte, [][8]byte) {
	r := rand.New(rand.NewPCG(s.Seed, 0x5eed))
	base := float64(s.Bitrate) / float64(s.FPS) / 8
	frames := make([]media.Frame, 0, s.Frames)
	digests := make([][8]byte, 0, s.Frames)
	for len(frames) < s.Frames {
		gop := s.GOP
		if gop == 0 {
			gop = 20 + r.IntN(51)
		}
		gop = min(gop, s.Frames-len(frames))
		complexity := 0.5 + 1.3*r.Float64()
		unit := base * complexity * float64(gop) / float64(gop+2)
		for j := range gop {
			i := uint32(len(frames))
			size := unit
			if j == 0 {
				size *= 3
			}
			payload := make([]byte, max(1, int(size)))
			var seed [32]byte
			binary.BigEndian.PutUint64(seed[:], s.Seed)
			binary.BigEndian.PutUint32(seed[8:], i)
			rand.NewChaCha8(seed).Read(payload)
			frames = append(frames, media.Frame{Index: i, Key: j == 0, Payload: payload})
			digests = append(digests, media.Digest(payload))
		}
	}
	if s.Corrupt == NoLeadingKeyframe {
		frames[0].Key = false
	}
	var b bytes.Buffer
	media.WriteRaw(&b, media.Header{Width: s.Width, Height: s.Height, FPS: s.FPS, Frames: uint32(len(frames))}, frames)
	data := b.Bytes()
	if s.Corrupt == Truncated {
		data = data[:len(data)*2/3]
	}
	return data, digests
}
