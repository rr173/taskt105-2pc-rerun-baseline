package httpapi

import "net/http"

// NewRouter returns an *http.ServeMux with the 2PC coordinator routes mounted.
// Go 1.22+ method-and-pattern routing binds each handler to a specific HTTP
// method, so unsupported methods fall through to the default 405 handler.
//
// Routes (23 endpoints):
//
//	resources:        POST   /resources
//	                  GET    /resources
//	                  GET    /resources/{name}
//	                  PUT    /resources/{name}
//	                  DELETE /resources/{name}
//	transactions:    POST   /txns
//	                  GET    /txns
//	                  GET    /txns/{id}
//	                  DELETE /txns/{id}
//	                  GET    /txns/{id}/participants
//	two-phase drive:  POST   /txns/{id}/prepare
//	                  POST   /txns/{id}/finish
//	                  POST   /txns/{id}/abort
//	                  POST   /txns/{id}/run
//	decisions:        GET    /decisions
//	                  GET    /decisions/{txn_id}
//	ledger:           GET    /ledger
//	                  GET    /ledger/{resource}
//	ops & recovery:   POST   /admin/recover
//	                  GET    /admin/recover/preview
//	                  GET    /admin/stats
//	health:           GET    /healthz
//	                  GET    /healthz/ready
func NewRouter(s *Server) http.Handler {
	mux := http.NewServeMux()
	// resources
	mux.HandleFunc("POST /resources", s.handleRegisterResource)
	mux.HandleFunc("GET /resources", s.handleListResources)
	mux.HandleFunc("GET /resources/{name}", s.handleGetResource)
	mux.HandleFunc("PUT /resources/{name}", s.handleUpdateResource)
	mux.HandleFunc("DELETE /resources/{name}", s.handleDeleteResource)
	// transactions
	mux.HandleFunc("POST /txns", s.handleBegin)
	mux.HandleFunc("GET /txns", s.handleListTxns)
	mux.HandleFunc("GET /txns/{id}", s.handleGetTxn)
	mux.HandleFunc("DELETE /txns/{id}", s.handleDeleteTxn)
	mux.HandleFunc("GET /txns/{id}/participants", s.handleListParticipants)
	// two-phase drive
	mux.HandleFunc("POST /txns/{id}/prepare", s.handlePrepare)
	mux.HandleFunc("POST /txns/{id}/finish", s.handleFinish)
	mux.HandleFunc("POST /txns/{id}/abort", s.handleAbort)
	mux.HandleFunc("POST /txns/{id}/run", s.handleRun)
	// decisions
	mux.HandleFunc("GET /decisions", s.handleListDecisions)
	mux.HandleFunc("GET /decisions/{txn_id}", s.handleGetDecision)
	// ledger
	mux.HandleFunc("GET /ledger", s.handleListLedger)
	mux.HandleFunc("GET /ledger/{resource}", s.handleListLedgerForResource)
	// ops & recovery
	mux.HandleFunc("POST /admin/recover", s.handleRecover)
	mux.HandleFunc("GET /admin/recover/preview", s.handleRecoverPreview)
	mux.HandleFunc("GET /admin/stats", s.handleStats)
	// health
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /healthz/ready", s.handleReady)
	mux.HandleFunc("GET /admin/analysis", s.handleAnalysis)
	return mux
}
