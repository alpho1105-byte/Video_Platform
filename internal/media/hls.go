package media

import (
	"bufio"
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Variant 是 primary（master）manifest 裡的一個畫質。Bandwidth 是最高的段落位元率，
// AverageBandwidth 是平均值；播放器以 Bandwidth 判斷頻寬夠不夠，避免在位元率高的段落卡住。
type Variant struct {
	URI              string
	Bandwidth        int
	AverageBandwidth int
	Width, Height    int
	Name             string
}

func WriteMaster(variants []Variant) []byte {
	var b bytes.Buffer
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n")
	for _, v := range variants {
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d,NAME=\"%s\"\n%s\n",
			v.Bandwidth, v.AverageBandwidth, v.Width, v.Height, v.Name, v.URI)
	}
	return b.Bytes()
}

func ParseMaster(data []byte) ([]Variant, error) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	if !sc.Scan() || sc.Text() != "#EXTM3U" {
		return nil, fmt.Errorf("%w: not an m3u8 playlist", ErrCorrupt)
	}
	var out []Variant
	var pending *Variant
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(line, "#EXT-X-STREAM-INF:"):
			v := Variant{}
			for _, attr := range splitAttrs(strings.TrimPrefix(line, "#EXT-X-STREAM-INF:")) {
				k, val, _ := strings.Cut(attr, "=")
				switch k {
				case "BANDWIDTH":
					v.Bandwidth, _ = strconv.Atoi(val)
				case "AVERAGE-BANDWIDTH":
					v.AverageBandwidth, _ = strconv.Atoi(val)
				case "RESOLUTION":
					w, h, _ := strings.Cut(val, "x")
					v.Width, _ = strconv.Atoi(w)
					v.Height, _ = strconv.Atoi(h)
				case "NAME":
					v.Name = strings.Trim(val, `"`)
				}
			}
			pending = &v
		case line == "" || strings.HasPrefix(line, "#"):
		case pending != nil:
			pending.URI = line
			out = append(out, *pending)
			pending = nil
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: master playlist has no variants", ErrCorrupt)
	}
	return out, nil
}

// splitAttrs 以逗號切開屬性，引號裡的逗號不切。
func splitAttrs(s string) []string {
	var out []string
	inQuote, start := false, 0
	for i, c := range s {
		switch {
		case c == '"':
			inQuote = !inQuote
		case c == ',' && !inQuote:
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return append(out, s[start:])
}

type MediaSegment struct {
	URI      string
	Duration float64 // 秒
}

// WriteMedia 寫出一個畫質的 media manifest（VOD：段落固定，結尾有 ENDLIST）。
func WriteMedia(segments []MediaSegment) []byte {
	target := 0.0
	for _, s := range segments {
		target = max(target, s.Duration)
	}
	var b bytes.Buffer
	fmt.Fprintf(&b, "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION:%d\n#EXT-X-MEDIA-SEQUENCE:0\n", int(math.Ceil(target)))
	for _, s := range segments {
		fmt.Fprintf(&b, "#EXTINF:%.3f,\n%s\n", s.Duration, s.URI)
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	return b.Bytes()
}

func ParseMedia(data []byte) ([]MediaSegment, error) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	if !sc.Scan() || sc.Text() != "#EXTM3U" {
		return nil, fmt.Errorf("%w: not an m3u8 playlist", ErrCorrupt)
	}
	var out []MediaSegment
	duration := -1.0
	ended := false
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case strings.HasPrefix(line, "#EXTINF:"):
			v, _, _ := strings.Cut(strings.TrimPrefix(line, "#EXTINF:"), ",")
			d, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: bad EXTINF %q", ErrCorrupt, line)
			}
			duration = d
		case line == "#EXT-X-ENDLIST":
			ended = true
		case line == "" || strings.HasPrefix(line, "#"):
		default:
			if duration < 0 {
				return nil, fmt.Errorf("%w: segment %q without EXTINF", ErrCorrupt, line)
			}
			out = append(out, MediaSegment{URI: line, Duration: duration})
			duration = -1
		}
	}
	if !ended || len(out) == 0 {
		return nil, fmt.Errorf("%w: incomplete media playlist", ErrCorrupt)
	}
	return out, nil
}
