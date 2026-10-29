package media

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
)

// Rendition 是一種輸出畫質。Bitrate 是目標平均位元率（bps）。
type Rendition struct {
	Name    string `json:"name"`
	Width   int    `json:"width"`
	Height  int    `json:"height"`
	Bitrate int    `json:"bitrate"`
}

// Ladder 是 bitrate ladder：由高到低。
var Ladder = []Rendition{
	{Name: "1080p", Width: 1920, Height: 1080, Bitrate: 2_500_000},
	{Name: "720p", Width: 1280, Height: 720, Bitrate: 1_200_000},
	{Name: "480p", Width: 854, Height: 480, Bitrate: 600_000},
	{Name: "240p", Width: 426, Height: 240, Bitrate: 250_000},
}

// LadderFor 只保留不高於原始解析度的畫質：把 720p 的影片放大成 1080p 只會浪費頻寬。
func LadderFor(h Header) []Rendition {
	var out []Rendition
	for _, r := range Ladder {
		if r.Height <= int(h.Height) {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		out = append(out, Ladder[len(Ladder)-1])
	}
	return out
}

func RenditionByName(name string) (Rendition, bool) {
	for _, r := range Ladder {
		if r.Name == name {
			return r, true
		}
	}
	return Rendition{}, false
}

// EncodedFrame 是轉碼後的 frame。SourceDigest 是原始 frame 的指紋，讓驗證端確認每個輸出 frame 都來自正確的來源。
type EncodedFrame struct {
	Index        uint32
	Key          bool
	SourceDigest [8]byte
	Payload      []byte
}

func Digest(payload []byte) [8]byte {
	sum := sha256.Sum256(payload)
	var d [8]byte
	copy(d[:], sum[:8])
	return d
}

// Transcode 把一個 frame 轉成指定畫質。輸出大小依「目標位元率 / 原始平均位元率」縮放，
// 所以畫面複雜（原始 frame 大）的段落輸出也大，各段的位元率會在平均值上下起伏，就像 VBR 編碼。
// 內容由原始 payload 與畫質決定，同一個輸入永遠得到同樣的輸出，轉碼重試因此是冪等的。
func Transcode(f Frame, r Rendition, sourceBitrate int) EncodedFrame {
	size := max(16, int(int64(len(f.Payload))*int64(r.Bitrate)/int64(max(sourceBitrate, 1))))
	seed := sha256.Sum256(append(append([]byte(nil), f.Payload...), r.Name...))
	payload := make([]byte, size)
	rand.NewChaCha8(seed).Read(payload)
	return EncodedFrame{Index: f.Index, Key: f.Key, SourceDigest: Digest(f.Payload), Payload: payload}
}

const segMagic = "VSEG"

// EncodeSegment 寫出一段輸出影片：magic、畫質名稱、frame 數，接著每個 frame 帶 CRC32。
func EncodeSegment(rendition string, frames []EncodedFrame) []byte {
	var b bytes.Buffer
	b.WriteString(segMagic)
	b.WriteByte(byte(len(rendition)))
	b.WriteString(rendition)
	binary.Write(&b, binary.BigEndian, uint32(len(frames)))
	for _, f := range frames {
		var hdr [21]byte
		binary.BigEndian.PutUint32(hdr[0:], f.Index)
		if f.Key {
			hdr[4] = 1
		}
		copy(hdr[5:13], f.SourceDigest[:])
		binary.BigEndian.PutUint32(hdr[13:], crc32.ChecksumIEEE(f.Payload))
		binary.BigEndian.PutUint32(hdr[17:], uint32(len(f.Payload)))
		b.Write(hdr[:])
		b.Write(f.Payload)
	}
	return b.Bytes()
}

// DecodeSegment 解出一段輸出影片並檢查每個 frame 的 CRC 與 frame index 連續。
func DecodeSegment(data []byte) (string, []EncodedFrame, error) {
	if len(data) < 9 || string(data[:4]) != segMagic {
		return "", nil, fmt.Errorf("%w: not a segment", ErrCorrupt)
	}
	n := int(data[4])
	if len(data) < 5+n+4 {
		return "", nil, fmt.Errorf("%w: short segment header", ErrCorrupt)
	}
	rendition := string(data[5 : 5+n])
	count := binary.BigEndian.Uint32(data[5+n:])
	pos := 9 + n
	frames := make([]EncodedFrame, 0, count)
	for i := range count {
		if len(data) < pos+21 {
			return "", nil, fmt.Errorf("%w: truncated frame header", ErrCorrupt)
		}
		f := EncodedFrame{Index: binary.BigEndian.Uint32(data[pos:]), Key: data[pos+4] == 1}
		copy(f.SourceDigest[:], data[pos+5:pos+13])
		crc := binary.BigEndian.Uint32(data[pos+13:])
		size := int(binary.BigEndian.Uint32(data[pos+17:]))
		pos += 21
		if len(data) < pos+size {
			return "", nil, fmt.Errorf("%w: truncated frame payload", ErrCorrupt)
		}
		f.Payload = data[pos : pos+size]
		pos += size
		if crc32.ChecksumIEEE(f.Payload) != crc {
			return "", nil, fmt.Errorf("%w: frame %d checksum mismatch", ErrCorrupt, f.Index)
		}
		if i > 0 && f.Index != frames[i-1].Index+1 {
			return "", nil, fmt.Errorf("%w: frame %d follows %d", ErrCorrupt, f.Index, frames[i-1].Index)
		}
		frames = append(frames, f)
	}
	if pos != len(data) {
		return "", nil, fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(data)-pos)
	}
	return rendition, frames, nil
}
