package client

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// Link 模擬使用者到 CDN edge 的網路：每個請求先等一個 RTT，下載速度受隨時間變化的頻寬限制。
type Link struct {
	RTT time.Duration
	// Bandwidth 回傳連線開始後經過 elapsed 時的頻寬（bps）。
	Bandwidth func(elapsed time.Duration) int

	start time.Time
	mu    sync.Mutex
	next  time.Time
}

func NewLink(rtt time.Duration, bandwidth func(time.Duration) int) *Link {
	return &Link{RTT: rtt, Bandwidth: bandwidth, start: time.Now()}
}

// Client 回傳經過這條連線的 HTTP client。
func (l *Link) Client() *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &throttledConn{Conn: c, link: l}, nil
		},
		MaxIdleConnsPerHost: 4,
		DisableCompression:  true,
	}
	return &http.Client{Transport: roundTripper{link: l, next: tr}, Timeout: time.Minute}
}

type roundTripper struct {
	link *Link
	next http.RoundTripper
}

func (rt roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	select {
	case <-r.Context().Done():
		return nil, r.Context().Err()
	case <-time.After(rt.link.RTT):
	}
	return rt.next.RoundTrip(r)
}

type throttledConn struct {
	net.Conn
	link *Link
}

// Read 每讀到 n 個位元組，就讓下一次讀取等到這些位元組依目前頻寬應該傳完的時間。
func (c *throttledConn) Read(p []byte) (int, error) {
	if len(p) > 16<<10 {
		p = p[:16<<10]
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.link.wait(n)
	}
	return n, err
}

func (l *Link) wait(n int) {
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	rate := max(l.Bandwidth(now.Sub(l.start)), 1000)
	l.next = l.next.Add(time.Duration(float64(n*8) / float64(rate) * float64(time.Second)))
	sleep := time.Until(l.next)
	l.mu.Unlock()
	if sleep > 0 {
		time.Sleep(sleep)
	}
}
