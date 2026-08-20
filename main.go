// Command task105-2pc runs the 2PC transaction-coordinator HTTP server. A
// single SQLite file is the source of truth; on startup the server reopens
// that file, runs a recovery pass to drive every non-terminal transaction to
// completion (PREPARING -> aborted, COMMITTING -> committed, ABORTING ->
// aborted), and only then begins serving the HTTP API. With --smoke-test it
// runs an in-process self-check against a temporary database and exits.
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

	"task105-2pc/internal/coordinator"
	"task105-2pc/internal/httpapi"
	"task105-2pc/internal/store"
)

// osExit is indirected so the smoke test can substitute it; in production it
// is os.Exit.
var osExit = os.Exit

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "2pc.db", "SQLite database path")
	smoke := flag.Bool("smoke-test", false, "run an in-process smoke test and exit")
	flag.Parse()

	if *smoke {
		if err := runSmokeTest(); err != nil {
			log.Println("smoke-test: FAIL:", err)
			osExit(1)
		}
		log.Println("smoke-test: ok")
		osExit(0)
	}

	srv, err := start(*dbPath, *addr)
	if err != nil {
		log.Fatalf("start: %v", err)
	}
	defer srv.store.Close()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	log.Printf("2pc coordinator listening on %s (db=%s)", *addr, *dbPath)
	<-stop
	log.Printf("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.http.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("shutdown: %v", err)
	}
}

// assembledServer holds the pieces needed to run and stop the service.
type assembledServer struct {
	store *store.Store
	coord *coordinator.Coordinator
	http  *http.Server
}

func start(dbPath, addr string) (*assembledServer, error) {
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, err
	}
	coord, err := coordinator.New(st, coordinator.RealClock{})
	if err != nil {
		st.Close()
		return nil, err
	}
	// Restart recovery: drive every non-terminal transaction to completion
	// before serving traffic. This is the 2PC durability guarantee — even
	// transactions left in-doubt by a crash are resolved.
	recs, err := coord.Recover(context.Background())
	if err != nil {
		st.Close()
		return nil, err
	}
	if len(recs) > 0 {
		log.Printf("startup recovery: completed %d in-doubt transaction(s)", len(recs))
	}
	srv := httpapi.New(coord)
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.NewRouter(srv),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()
	return &assembledServer{store: st, coord: coord, http: httpSrv}, nil
}
