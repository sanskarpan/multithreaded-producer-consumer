// Web UI main entry point
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sanskarpan/producer-consumer/internal/logging"
	"github.com/sanskarpan/producer-consumer/web/server"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP server address")
	flag.Parse()

	// Route stdlib log to our structured logger so any other libs log uniformly.
	log.SetFlags(0)
	log.SetOutput(logging.LogWriter())

	logging.L().Info("starting producer-consumer web UI", "addr", *addr)
	log.Println("Open http://localhost" + *addr + " in your browser")

	srv := server.NewServer()

	// Install a signal handler so SIGINT/SIGTERM stops any running pattern
	// gracefully and shuts the HTTP server down.
	idleConnsClosed := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		s := <-sigCh
		logging.L().Info("shutdown signal received", "signal", s.String())

		// 10s is generous; the active pattern cancels immediately and the
		// HTTP server has a similar grace period before forcibly closing
		// remaining connections.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logging.L().Error("graceful shutdown failed", "err", err)
		}
		close(idleConnsClosed)
	}()

	if err := srv.Start(*addr); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logging.L().Error("server error", "err", err)
		log.Fatalf("server error: %v", err)
	}

	<-idleConnsClosed
	logging.L().Info("server stopped cleanly")
}
