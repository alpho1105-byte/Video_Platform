package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"video-platform/internal/domain"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func startVideo(t *testing.T, st *Store, id string, tasks ...TaskSpec) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreateVideo(ctx, domain.Video{ID: id, Uploader: "u", Title: id, Size: 1, PartSize: 1, Status: domain.StatusUploading, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.StartProcessing(ctx, id, "etag", tasks); err != nil {
		t.Fatal(err)
	}
}

// 尖峰時 ready 的任務源源不絕；租約過期的任務必須和它們依「可以執行的時間」一起排隊，不能等 ready 的都做完。
func TestExpiredLeaseIsReclaimedBeforeNewerReadyTasks(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	startVideo(t, st, "v1", TaskSpec{ID: "v1:a", Kind: "k", MaxAttempts: 3})
	first, ok, err := st.Lease(ctx, "crashed", 30*time.Millisecond)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	time.Sleep(60 * time.Millisecond)
	startVideo(t, st, "v2", TaskSpec{ID: "v2:a", Kind: "k", MaxAttempts: 3}, TaskSpec{ID: "v2:b", Kind: "k", MaxAttempts: 3})

	again, ok, err := st.Lease(ctx, "survivor", time.Minute)
	if err != nil || !ok || again.ID != "v1:a" || again.Attempt != 2 || again.Token == first.Token {
		t.Fatalf("leased %+v, %v", again, err)
	}
	if err := st.Complete(ctx, first.ID, first.Token, nil, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("the crashed worker completed with a stale token: %v", err)
	}
	if err := st.Complete(ctx, again.ID, again.Token, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCompletingTheLastDependencyMakesTheJoinReady(t *testing.T) {
	st := openStore(t)
	ctx := context.Background()
	startVideo(t, st, "v1",
		TaskSpec{ID: "v1:a", Kind: "k", MaxAttempts: 1},
		TaskSpec{ID: "v1:b", Kind: "k", MaxAttempts: 1},
		TaskSpec{ID: "v1:join", Kind: "join", After: []string{"v1:a", "v1:b"}, MaxAttempts: 1})
	for range 2 {
		task, ok, _ := st.Lease(ctx, "w", time.Minute)
		if !ok || task.Kind != "k" {
			t.Fatalf("leased %+v before its dependencies finished", task)
		}
		if err := st.Complete(ctx, task.ID, task.Token, task.ID+" output", nil); err != nil {
			t.Fatal(err)
		}
	}
	join, ok, _ := st.Lease(ctx, "w", time.Minute)
	if !ok || join.ID != "v1:join" || len(join.Inputs) != 2 || string(join.Inputs["v1:a"]) != `"v1:a output"` {
		t.Fatalf("join = %+v", join)
	}
}
