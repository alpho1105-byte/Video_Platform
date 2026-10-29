// Package media 是這個專案的簡化影片格式、切段、轉碼與 HLS manifest。
//
// 沒有真的壓縮影像：一個 frame 就是一段 payload，轉碼依目標位元率縮放 payload 的大小並重新產生內容，
// 但保留真實系統需要面對的結構：keyframe、GOP、依 keyframe 切段、多個 rendition 的段落對齊、
// 位元率隨畫面複雜度起伏。
package media

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var ErrCorrupt = errors.New("corrupt media")

const (
	rawMagic     = "VRAW"
	maxFrameSize = 4 << 20
)

// Header 描述原始影片。Frames 是這個檔案裡的 frame 數。
type Header struct {
	Width  uint16
	Height uint16
	FPS    uint16
	Frames uint32
}

type Frame struct {
	Index   uint32
	Key     bool
	Payload []byte
}

// WriteRaw 寫出原始影片：magic、header，接著依序是每個 frame 的 index、keyframe 旗標、長度與 payload。
func WriteRaw(w io.Writer, h Header, frames []Frame) error {
	bw := bufio.NewWriter(w)
	bw.WriteString(rawMagic)
	binary.Write(bw, binary.BigEndian, h)
	for _, f := range frames {
		if err := writeRawFrame(bw, f); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func writeRawFrame(w io.Writer, f Frame) error {
	var hdr [9]byte
	binary.BigEndian.PutUint32(hdr[0:], f.Index)
	if f.Key {
		hdr[4] = 1
	}
	binary.BigEndian.PutUint32(hdr[5:], uint32(len(f.Payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(f.Payload)
	return err
}

// RawReader 逐一讀出 frame，不必把整支影片放進記憶體。
type RawReader struct {
	r      *bufio.Reader
	header Header
	read   uint32
	first  uint32
}

func NewRawReader(r io.Reader) (*RawReader, error) {
	br := bufio.NewReaderSize(r, 64<<10)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(br, magic); err != nil || string(magic) != rawMagic {
		return nil, fmt.Errorf("%w: not a raw video", ErrCorrupt)
	}
	var h Header
	if err := binary.Read(br, binary.BigEndian, &h); err != nil {
		return nil, fmt.Errorf("%w: header: %v", ErrCorrupt, err)
	}
	if h.FPS == 0 || h.Width == 0 || h.Height == 0 {
		return nil, fmt.Errorf("%w: invalid header %+v", ErrCorrupt, h)
	}
	return &RawReader{r: br, header: h}, nil
}

func (r *RawReader) Header() Header { return r.header }

// Next 回傳下一個 frame；讀完 header 宣告的 frame 數後回傳 io.EOF。
// frame index 必須連續，檔案提早結束視為損毀。
func (r *RawReader) Next() (Frame, error) {
	if r.read == r.header.Frames {
		return Frame{}, io.EOF
	}
	var hdr [9]byte
	if _, err := io.ReadFull(r.r, hdr[:]); err != nil {
		return Frame{}, fmt.Errorf("%w: truncated at frame %d", ErrCorrupt, r.read)
	}
	f := Frame{Index: binary.BigEndian.Uint32(hdr[0:]), Key: hdr[4] == 1}
	if r.read == 0 {
		r.first = f.Index
	}
	if f.Index != r.first+r.read {
		return Frame{}, fmt.Errorf("%w: frame %d where %d was expected", ErrCorrupt, f.Index, r.first+r.read)
	}
	n := binary.BigEndian.Uint32(hdr[5:])
	if n > maxFrameSize {
		return Frame{}, fmt.Errorf("%w: frame %d claims %d bytes", ErrCorrupt, f.Index, n)
	}
	f.Payload = make([]byte, n)
	if _, err := io.ReadFull(r.r, f.Payload); err != nil {
		return Frame{}, fmt.Errorf("%w: truncated in frame %d", ErrCorrupt, f.Index)
	}
	r.read++
	return f, nil
}
