package media

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func frames(n, gop int, size func(i int) int) []Frame {
	out := make([]Frame, n)
	for i := range out {
		p := make([]byte, size(i))
		for j := range p {
			p[j] = byte(i + j)
		}
		out[i] = Frame{Index: uint32(i), Key: i%gop == 0, Payload: p}
	}
	return out
}

func rawVideo(t *testing.T, h Header, fs []Frame) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := WriteRaw(&b, h, fs); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestSplitCutsOnKeyframesAfterTheTarget(t *testing.T) {
	// 30fps，GOP 45 frame（1.5 秒）：目標 2 秒，所以每段是兩個 GOP、3 秒，最後一段剩下的部分。
	data := rawVideo(t, Header{Width: 1920, Height: 1080, FPS: 30, Frames: 200}, frames(200, 45, func(int) int { return 10 }))
	r, err := NewRawReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	var segs []SegmentInfo
	err = Split(r, 2*time.Second, func(s SegmentInfo, fs []Frame) error {
		if !fs[0].Key || fs[0].Index != s.FirstFrame || len(fs) != s.Frames {
			t.Fatalf("segment %+v starts with %+v", s, fs[0].Index)
		}
		segs = append(segs, s)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []int{90, 90, 20}
	if len(segs) != len(want) {
		t.Fatalf("segments = %+v", segs)
	}
	for i, s := range segs {
		if s.Frames != want[i] || s.Index != i {
			t.Fatalf("segment %d = %+v, want %d frames", i, s, want[i])
		}
	}
	if segs[0].Duration(30) != 3*time.Second {
		t.Fatalf("duration = %s", segs[0].Duration(30))
	}
}

func TestCorruptSourcesAreRejected(t *testing.T) {
	good := frames(60, 30, func(int) int { return 8 })
	notKey := append([]Frame(nil), good...)
	notKey[0].Key = false
	h := Header{Width: 640, Height: 360, FPS: 30, Frames: 60}
	cases := map[string][]byte{
		"first frame is not a keyframe": rawVideo(t, h, notKey),
		"truncated":                     rawVideo(t, h, good)[:300],
		"not a video":                   []byte("hello world, definitely not a video"),
	}
	for name, data := range cases {
		r, err := NewRawReader(bytes.NewReader(data))
		if err == nil {
			err = Split(r, 2*time.Second, func(SegmentInfo, []Frame) error { return nil })
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestTranscodeIsDeterministicAndTracksTheSourceBitrate(t *testing.T) {
	src := Frame{Index: 7, Key: true, Payload: bytes.Repeat([]byte{1, 2, 3}, 1000)}
	a := Transcode(src, Ladder[1], 2_400_000)
	b := Transcode(src, Ladder[1], 2_400_000)
	if !bytes.Equal(a.Payload, b.Payload) || len(a.Payload) != 1500 || a.SourceDigest != Digest(src.Payload) {
		t.Fatalf("size %d, deterministic %v", len(a.Payload), bytes.Equal(a.Payload, b.Payload))
	}
	if c := Transcode(src, Ladder[2], 2_400_000); bytes.Equal(c.Payload[:16], a.Payload[:16]) {
		t.Fatal("different renditions produced the same bytes")
	}
}

func TestSegmentRoundTripDetectsCorruption(t *testing.T) {
	fs := []EncodedFrame{
		{Index: 60, Key: true, Payload: []byte("keyframe")},
		{Index: 61, Payload: []byte("delta")},
	}
	data := EncodeSegment("720p", fs)
	name, back, err := DecodeSegment(data)
	if err != nil || name != "720p" || len(back) != 2 || back[1].Index != 61 || string(back[0].Payload) != "keyframe" {
		t.Fatalf("decode = %q %+v %v", name, back, err)
	}
	data[len(data)-1] ^= 0xff
	if _, _, err := DecodeSegment(data); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("flipped byte: %v", err)
	}
}

func TestPlaylistsRoundTrip(t *testing.T) {
	master := WriteMaster([]Variant{{URI: "720p/index.m3u8", Bandwidth: 1_500_000, AverageBandwidth: 1_200_000, Width: 1280, Height: 720, Name: "720p"}})
	vs, err := ParseMaster(master)
	if err != nil || len(vs) != 1 || vs[0].Bandwidth != 1_500_000 || vs[0].Height != 720 || vs[0].Name != "720p" || vs[0].URI != "720p/index.m3u8" {
		t.Fatalf("master = %+v %v", vs, err)
	}
	media := WriteMedia([]MediaSegment{{URI: "seg_00000.seg", Duration: 3}, {URI: "seg_00001.seg", Duration: 0.667}})
	if !bytes.Contains(media, []byte("#EXT-X-TARGETDURATION:3")) {
		t.Fatalf("media playlist:\n%s", media)
	}
	segs, err := ParseMedia(media)
	if err != nil || len(segs) != 2 || segs[1].Duration != 0.667 {
		t.Fatalf("media = %+v %v", segs, err)
	}
	if _, err := ParseMedia(bytes.TrimSuffix(media, []byte("#EXT-X-ENDLIST\n"))); err == nil {
		t.Fatal("a VOD playlist without ENDLIST was accepted")
	}
}

func TestLadderSkipsUpscaling(t *testing.T) {
	if got := LadderFor(Header{Height: 720}); len(got) != 3 || got[0].Name != "720p" {
		t.Fatalf("ladder for 720p = %+v", got)
	}
}
