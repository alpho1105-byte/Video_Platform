package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"video-platform/internal/app"
	"video-platform/internal/cdn"
)

func main() {
	addr := flag.String("addr", ":8099", "Video Service listen address")
	data := flag.String("data", "data", "directory for the database and blob objects")
	host := flag.String("host", "127.0.0.1", "host the blob store and CDN edges listen on")
	blobPort := flag.Int("blob-port", 8120, "blob store port")
	edgePort := flag.Int("edge-port", 8131, "port of the first CDN edge")
	dev := flag.Bool("dev", true, "enable POST /dev/token")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(*addr, *data, *host, *blobPort, *edgePort, *dev, logger); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func secret(name, fallback string, dev bool, logger *slog.Logger) ([]byte, error) {
	if v := os.Getenv(name); v != "" {
		return []byte(v), nil
	}
	if !dev {
		return nil, fmt.Errorf("%s is required when -dev=false", name)
	}
	logger.Warn(name + " not set, using an insecure development secret")
	return []byte(fallback), nil
}

func run(addr, data, host string, blobPort, edgePort int, dev bool, logger *slog.Logger) error {
	jwtSecret, err := secret("JWT_SECRET", "dev-only-secret", dev, logger)
	if err != nil {
		return err
	}
	blobKey, err := secret("BLOB_KEY", "dev-only-blob-key", dev, logger)
	if err != nil {
		return err
	}
	blobLn, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, blobPort))
	if err != nil {
		return err
	}
	opts := app.DefaultOptions(data, jwtSecret, blobKey)
	opts.Dev = dev
	opts.Logger = logger
	a, err := app.New(opts, blobLn)
	if err != nil {
		return err
	}
	defer a.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	a.Start(ctx)

	servers := []*http.Server{}
	for i, region := range []struct {
		name    string
		latency time.Duration
	}{{"tw", 10 * time.Millisecond}, {"us", 60 * time.Millisecond}, {"eu", 120 * time.Millisecond}} {
		edge := cdn.New(cdn.Config{Region: region.name, Capacity: 512 << 20, OriginLatency: region.latency}, a.BlobBase)
		srv := &http.Server{Addr: fmt.Sprintf("%s:%d", host, edgePort+i), Handler: edge.Handler(), ReadHeaderTimeout: 5 * time.Second}
		servers = append(servers, srv)
		go srv.ListenAndServe()
	}
	handler, _ := a.NewAPI()
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	servers = append(servers, srv)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, s := range servers {
			s.Shutdown(shutdownCtx)
		}
	}()

	logger.Info("video service listening", "addr", addr, "blob", a.BlobBase, "edges", fmt.Sprintf("%s:%d-%d", host, edgePort, edgePort+2))
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
