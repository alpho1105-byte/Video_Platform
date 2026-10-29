package blob

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type rig struct {
	t      *testing.T
	srv    *httptest.Server
	client *Client
	signer *Signer
}

func newRig(t *testing.T) *rig {
	t.Helper()
	fs, err := NewFS(t.TempDir(), 4)
	if err != nil {
		t.Fatal(err)
	}
	signer := NewSigner([]byte("blob-secret"))
	srv := httptest.NewServer(NewServer(fs, signer, ServerConfig{MaxPart: 64, MaxObject: 1 << 20}).Handler())
	t.Cleanup(srv.Close)
	return &rig{t: t, srv: srv, client: NewClient(srv.URL, signer), signer: signer}
}

func (r *rig) put(rawURL string, body string) (int, string) {
	r.t.Helper()
	req, _ := http.NewRequest(http.MethodPut, rawURL, strings.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		r.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode, strings.Trim(resp.Header.Get("ETag"), `"`)
}

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestMultipartUploadAssemblesPartsInOrder(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	id, err := r.client.CreateMultipart(ctx, "raw/v1")
	if err != nil {
		t.Fatal(err)
	}
	// 亂序上傳，而且 part 2 傳了兩次：以最後一次為準。
	for _, p := range []struct {
		n    int
		body string
	}{{3, "ccc"}, {1, "aaaa"}, {2, "xxxx"}, {2, "bbbb"}} {
		if code, etag := r.put(r.client.PresignPart("raw/v1", id, p.n, time.Minute), p.body); code != 200 || etag != md5hex(p.body) {
			t.Fatalf("part %d: %d %q", p.n, code, etag)
		}
	}
	parts, _ := r.client.ListParts(ctx, "raw/v1", id)
	if len(parts) != 3 {
		t.Fatalf("parts = %+v", parts)
	}
	want := []Part{{PartNumber: 1, ETag: md5hex("aaaa")}, {PartNumber: 2, ETag: md5hex("bbbb")}, {PartNumber: 3, ETag: md5hex("ccc")}}
	if _, err := r.client.Complete(ctx, "raw/v1", id, []Part{want[0], {PartNumber: 2, ETag: md5hex("xxxx")}, want[2]}); err == nil {
		t.Fatal("completed with a stale ETag for part 2")
	}
	obj, err := r.client.Complete(ctx, "raw/v1", id, want)
	if err != nil || obj.Size != 11 || !strings.HasSuffix(obj.ETag, "-3") {
		t.Fatalf("complete = %+v %v", obj, err)
	}
	if again, err := r.client.Complete(ctx, "raw/v1", id, want); err != nil || again != obj {
		t.Fatalf("second complete = %+v %v", again, err)
	}
	data, _ := r.client.Get(ctx, "raw/v1")
	if string(data) != "aaaabbbbccc" {
		t.Fatalf("object = %q", data)
	}
}

func TestPartsBelowTheMinimumSizeAreRejected(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	id, _ := r.client.CreateMultipart(ctx, "raw/v2")
	r.put(r.client.PresignPart("raw/v2", id, 1, time.Minute), "ab")
	r.put(r.client.PresignPart("raw/v2", id, 2, time.Minute), "cdef")
	_, err := r.client.Complete(ctx, "raw/v2", id, []Part{{PartNumber: 1, ETag: md5hex("ab")}, {PartNumber: 2, ETag: md5hex("cdef")}})
	if err == nil || !strings.Contains(err.Error(), "minimum") {
		t.Fatalf("small first part: %v", err)
	}
}

func TestPresignedURLsCannotBeReusedForSomethingElse(t *testing.T) {
	r := newRig(t)
	id, _ := r.client.CreateMultipart(context.Background(), "raw/v3")
	good := r.client.PresignPart("raw/v3", id, 1, time.Minute)
	u, _ := url.Parse(good)

	other := *u
	q := other.Query()
	q.Set("partNumber", "2")
	other.RawQuery = q.Encode()
	moved := strings.Replace(good, "raw/v3", "raw/v4", 1)
	expired := r.client.PresignPart("raw/v3", id, 1, -time.Second)
	for name, target := range map[string]string{"another part": other.String(), "another object": moved, "expired": expired} {
		if code, _ := r.put(target, "data"); code != http.StatusForbidden {
			t.Errorf("%s: HTTP %d", name, code)
		}
	}
	if code, _ := r.put(good, strings.Repeat("x", 65)); code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized part: HTTP %d", code)
	}
	resp, _ := http.Get(r.srv.URL + "/o/raw/v3")
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("unsigned read of a private object: HTTP %d", resp.StatusCode)
	}
}

func TestPublicPrefixIsReadableAndCacheable(t *testing.T) {
	r := newRig(t)
	ctx := context.Background()
	if _, err := r.client.Put(ctx, "vod/v1/master.m3u8", []byte("#EXTM3U\n")); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(r.srv.URL + "/o/vod/v1/master.m3u8")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(body, []byte("#EXTM3U\n")) || !strings.Contains(resp.Header.Get("Cache-Control"), "immutable") {
		t.Fatalf("public read: %d %q %q", resp.StatusCode, body, resp.Header.Get("Cache-Control"))
	}
	r.client.Put(ctx, "vod/v1/a/1.seg", []byte("1"))
	if n, err := r.client.DeletePrefix(ctx, "vod/v1"); err != nil || n != 2 {
		t.Fatalf("delete prefix = %d, %v", n, err)
	}
	if _, err := r.client.Get(ctx, "vod/v1/master.m3u8"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}
