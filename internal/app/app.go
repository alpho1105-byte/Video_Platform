package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"video-platform/internal/api"
	"video-platform/internal/auth"
	"video-platform/internal/blob"
	"video-platform/internal/processing"
	"video-platform/internal/store"
	"video-platform/internal/videos"
	"video-platform/internal/workflow"
)

type Options struct {
	DBPath  string
	BlobDir string
	// Secret 簽發使用者的 JWT；BlobKey 是服務存取 blob store 的金鑰，presigned URL 也用它簽。
	Secret       []byte
	BlobKey      []byte
	MinPart      int64
	MaxPart      int64
	CacheEntries int
	Dev          bool
	Videos       videos.Config
	Processing   processing.Config
	Workflow     workflow.Config
	Pool         workflow.PoolConfig
	Logger       *slog.Logger
}

func DefaultOptions(dataDir string, secret, blobKey []byte) Options {
	return Options{DBPath: dataDir + "/videos.db", BlobDir: dataDir + "/blob", Secret: secret, BlobKey: blobKey,
		MinPart: 5 << 20, MaxPart: 64 << 20, CacheEntries: 10000,
		Videos: videos.DefaultConfig(), Processing: processing.DefaultConfig(), Workflow: workflow.DefaultConfig(),
		Pool:   workflow.PoolConfig{Min: 2, Max: 16, PerWorker: 8, Interval: time.Second, ScaleDownAfter: 10 * time.Second},
		Logger: slog.Default()}
}

// App 組裝所有元件。正式環境裡 blob store 是 S3、metadata 是分區的 NoSQL、轉檔 worker 是獨立擴展的機群；
// 這裡放在同一個 process，但 blob store 有自己的 listener，client 一樣直接用 presigned URL 對它上傳。
type App struct {
	opts      Options
	Store     *store.Store
	Blob      *blob.Server
	BlobBase  string
	BlobAdmin *blob.Client
	Processor *processing.Processor
	Engine    *workflow.Engine
	Pool      *workflow.Pool
	signer    *auth.Signer
	blobSrv   *http.Server
}

// New 開啟資料庫，並在 blobLn 上啟動 blob store。
func New(opts Options, blobLn net.Listener) (*App, error) {
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}
	fs, err := blob.NewFS(opts.BlobDir, opts.MinPart)
	if err != nil {
		st.Close()
		return nil, err
	}
	signer := blob.NewSigner(opts.BlobKey)
	a := &App{opts: opts, Store: st, signer: auth.NewSigner(opts.Secret),
		Blob:     blob.NewServer(fs, signer, blob.ServerConfig{MaxPart: opts.MaxPart, MaxObject: 1 << 30}),
		BlobBase: "http://" + blobLn.Addr().String()}
	a.blobSrv = &http.Server{Handler: a.Blob.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go a.blobSrv.Serve(blobLn)
	a.BlobAdmin = blob.NewClient(a.BlobBase, signer)
	a.Processor = processing.New(opts.Processing, a.BlobAdmin, st)
	a.Engine = workflow.New(st, opts.Workflow, a.Processor.Handlers(), opts.Logger.With("component", "workflow"))
	a.Pool = workflow.NewPool(a.Engine, st, opts.Pool)
	return a, nil
}

// Start 啟動轉檔 worker pool。
func (a *App) Start(ctx context.Context) {
	go a.Pool.Run(ctx)
}

// NewAPI 建立一個 Video Service 實例。實例之間不共享任何狀態（各自有 metadata 快取），可以放在負載平衡器後面任意擴展。
func (a *App) NewAPI() (http.Handler, *videos.Cache) {
	cache := videos.NewCache(a.opts.CacheEntries)
	svc := videos.New(a.opts.Videos, a.Store, a.BlobAdmin, a.Processor.Root, cache)
	return api.NewRouter(api.Deps{Signer: a.signer, Videos: svc, Dev: a.opts.Dev, Log: a.opts.Logger.With("component", "api")}), cache
}

func (a *App) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := a.blobSrv.Shutdown(ctx)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	return errors.Join(err, a.Store.Close())
}
