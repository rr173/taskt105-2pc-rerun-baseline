// Package httpapi exposes the 2PC transaction coordinator over JSON/HTTP.
// It depends only on the coordinator domain package and the standard library;
// the net/http mux (Go 1.22+ method patterns) is used directly so no
// third-party router is pulled in.
//
// Error mapping:
//   - store.ErrNotFound              -> 404 (txn not found)
//   - store.ErrResourceNotFound      -> 404 (resource not found)
//   - store.ErrResourceExists        -> 409 (resource exists)
//   - store.ErrTxnExists             -> 409 (txn exists)
//   - store.ErrResourceMissing        -> 400 (referenced resource not registered)
//   - store.ErrResourceInUse         -> 409 (resource in use, with active txns)
//   - store.ErrInvalidState          -> 409 (with current state in the body)
//   - store.ErrNoDecision            -> 409 PREPARING
//   - store.ErrNotTerminal           -> 409 (txn not terminal)
//   - input validation errors        -> 400
//   - anything else                  -> 500
package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"task105-2pc/internal/coordinator"
	"task105-2pc/internal/store"
)

// Server is the HTTP front-end for a coordinator.Coordinator.
type Server struct {
	coord *coordinator.Coordinator
}

// New returns a Server wired to coord.
func New(coord *coordinator.Coordinator) *Server { return &Server{coord: coord} }

// ---- request / response DTOs ------------------------------------------------

type registerResourceRequest struct {
	Name string `json:"name"`
	Vote string `json:"vote"`
}

type updateResourceRequest struct {
	Vote string `json:"vote"`
}

type resourceDTO struct {
	Name           string `json:"name"`
	Vote           string `json:"vote"`
	CommittedCount int64  `json:"committed_count"`
	AbortedCount   int64  `json:"aborted_count"`
	CreatedAt      string `json:"created_at"`
}

type beginRequest struct {
	TxnID     string   `json:"txn_id"`
	Resources []string `json:"resources"`
}

type txnSummaryDTO struct {
	TxnID     string `json:"txn_id"`
	State     string `json:"state"`
	Decision  string `json:"decision"`
	StartedAt string `json:"started_at"`
	DecidedAt string `json:"decided_at"`
}

type participantDTO struct {
	Resource    string `json:"resource"`
	Vote        string `json:"vote"`
	Final       string `json:"final"`
	VotedAt     string `json:"voted_at"`
	FinalizedAt string `json:"finalized_at"`
}

type txnDetailDTO struct {
	TxnID        string           `json:"txn_id"`
	Resources    []string         `json:"resources"`
	State        string           `json:"state"`
	Decision     string           `json:"decision"`
	StartedAt    string           `json:"started_at"`
	DecidedAt    string           `json:"decided_at"`
	Participants []participantDTO `json:"participants"`
}

type prepareResponse struct {
	TxnID    string            `json:"txn_id"`
	Decision string            `json:"decision"`
	State    string            `json:"state"`
	Votes    map[string]string `json:"votes"`
}

type finishResponse struct {
	TxnID        string           `json:"txn_id"`
	State        string           `json:"state"`
	Participants []participantDTO `json:"participants"`
}

type runResponse struct {
	TxnID        string           `json:"txn_id"`
	Decision     string           `json:"decision"`
	State        string           `json:"state"`
	Participants []participantDTO `json:"participants"`
}

type decisionDTO struct {
	TxnID      string `json:"txn_id"`
	Decision   string `json:"decision"`
	RecordedAt string `json:"recorded_at"`
}

type ledgerDTO struct {
	Resource  string `json:"resource"`
	TxnID     string `json:"txn_id"`
	AppliedAt string `json:"applied_at"`
}

type recoverResponse struct {
	Recovered []recoveryDTO `json:"recovered"`
}

type recoveryDTO struct {
	TxnID     string `json:"txn_id"`
	FromState string `json:"from_state"`
	ToState   string `json:"to_state"`
}

type recoverPreviewResponse struct {
	Pending []recoverPreviewDTO `json:"pending"`
}

type recoverPreviewDTO struct {
	TxnID        string `json:"txn_id"`
	CurrentState string `json:"current_state"`
	TargetState  string `json:"target_state"`
}

type statsDTO struct {
	TxnCount      int            `json:"txns"`
	ByState       map[string]int `json:"by_state"`
	ResourceCount int            `json:"resources"`
	LedgerCount   int            `json:"ledger_entries"`
	DecisionCount int            `json:"decisions"`
}

type okResponse struct {
	Deleted bool   `json:"deleted"`
	Name    string `json:"name,omitempty"`
}

// ---- helpers ----------------------------------------------------------------

// writeJSON writes v as JSON with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError maps a coordinator/store error to an HTTP status and writes
// {"error": <reason>}. ErrResourceInUse is handled by the deleting handler
// (which has the active txn ids); other errors map via mapError.
func writeError(w http.ResponseWriter, err error) {
	status, msg := mapError(err)
	writeJSON(w, status, map[string]string{"error": msg})
}

// mapError maps an error to (status, message). For ErrInvalidState the message
// carries the current state so the caller knows why the op was refused.
func mapError(err error) (int, string) {
	switch {
	case err == nil:
		return http.StatusOK, ""
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "txn not found"
	case errors.Is(err, store.ErrResourceNotFound):
		return http.StatusNotFound, "resource not found"
	case errors.Is(err, store.ErrResourceExists):
		return http.StatusConflict, "resource exists"
	case errors.Is(err, store.ErrTxnExists):
		return http.StatusConflict, "txn exists"
	case errors.Is(err, store.ErrResourceMissing):
		return http.StatusBadRequest, "resource not registered"
	case errors.Is(err, store.ErrInvalidState):
		return http.StatusConflict, err.Error()
	case errors.Is(err, store.ErrNoDecision):
		return http.StatusConflict, "PREPARING"
	case errors.Is(err, store.ErrNotTerminal):
		return http.StatusConflict, "txn not terminal"
	}
	msg := err.Error()
	if isValidationError(err) {
		return http.StatusBadRequest, msg
	}
	return http.StatusInternalServerError, msg
}

// isValidationError recognises sentinel validation errors produced by the
// coordinator (empty name, bad vote, empty txn_id, empty resources). They are
// created with errors.New so we match by message substring.
func isValidationError(err error) bool {
	msg := err.Error()
	for _, p := range []string{"is empty", "must be", "vote"} {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

// decodeJSON decodes r.Body into v. On failure it writes a 400 and returns
// false; the caller must return early.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "json: " + err.Error()})
		return false
	}
	return true
}

func pathID(r *http.Request) string       { return r.PathValue("id") }
func pathName(r *http.Request) string     { return r.PathValue("name") }
func pathTxnID(r *http.Request) string    { return r.PathValue("txn_id") }
func pathResource(r *http.Request) string { return r.PathValue("resource") }

// ---- resource handlers ------------------------------------------------------

func (s *Server) handleRegisterResource(w http.ResponseWriter, r *http.Request) {
	var req registerResourceRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	vote := req.Vote
	if vote == "" {
		vote = store.VoteYes
	}
	if err := s.coord.RegisterResource(r.Context(), req.Name, vote); err != nil {
		writeError(w, err)
		return
	}
	row, _, _ := s.coord.GetResource(r.Context(), req.Name)
	writeJSON(w, http.StatusOK, toResourceDTO(row))
}

func (s *Server) handleListResources(w http.ResponseWriter, r *http.Request) {
	rows, err := s.coord.ListResources(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]resourceDTO, 0, len(rows))
	for _, row := range rows {
		out = append(out, toResourceDTO(row))
	}
	writeJSON(w, http.StatusOK, map[string]any{"resources": out})
}

func (s *Server) handleGetResource(w http.ResponseWriter, r *http.Request) {
	row, ok, err := s.coord.GetResource(r.Context(), pathName(r))
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "resource not found"})
		return
	}
	writeJSON(w, http.StatusOK, toResourceDTO(row))
}

func (s *Server) handleUpdateResource(w http.ResponseWriter, r *http.Request) {
	var req updateResourceRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	name := pathName(r)
	if err := s.coord.UpdateResourceVote(r.Context(), name, req.Vote); err != nil {
		writeError(w, err)
		return
	}
	row, _, _ := s.coord.GetResource(r.Context(), name)
	writeJSON(w, http.StatusOK, toResourceDTO(row))
}

func (s *Server) handleDeleteResource(w http.ResponseWriter, r *http.Request) {
	name := pathName(r)
	active, err := s.coord.DeleteResource(r.Context(), name)
	if err != nil {
		// If the resource is in use, surface the active txn ids that the
		// coordinator already computed (txns in PREPARING/COMMITTING/ABORTING
		// that reference this resource).
		if errors.Is(err, store.ErrResourceInUse) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":       "resource in use",
				"active_txns": active,
			})
			return
		}
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, okResponse{Deleted: true, Name: name})
}

// ---- transaction handlers ---------------------------------------------------

func (s *Server) handleBegin(w http.ResponseWriter, r *http.Request) {
	var req beginRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if err := s.coord.Begin(r.Context(), req.TxnID, req.Resources); err != nil {
		writeError(w, err)
		return
	}
	t, _, _ := s.coord.GetTxn(r.Context(), req.TxnID)
	writeJSON(w, http.StatusOK, map[string]any{
		"txn_id":     t.TxnID,
		"resources":  t.Resources,
		"state":      t.State,
		"started_at": formatUnixNano(t.StartedAt),
	})
}

func (s *Server) handleListTxns(w http.ResponseWriter, r *http.Request) {
	stateFilter := r.URL.Query().Get("state")
	txns, err := s.coord.ListTxns(r.Context(), stateFilter)
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]txnSummaryDTO, 0, len(txns))
	for _, t := range txns {
		out = append(out, txnSummaryDTO{
			TxnID:     t.TxnID,
			State:     t.State,
			Decision:  t.Decision,
			StartedAt: formatUnixNano(t.StartedAt),
			DecidedAt: formatUnixNano(t.DecidedAt),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"txns": out})
}

func (s *Server) handleGetTxn(w http.ResponseWriter, r *http.Request) {
	t, parts, err := s.coord.GetTxn(r.Context(), pathID(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, txnDetailDTO{
		TxnID:        t.TxnID,
		Resources:    t.Resources,
		State:        t.State,
		Decision:     t.Decision,
		StartedAt:    formatUnixNano(t.StartedAt),
		DecidedAt:    formatUnixNano(t.DecidedAt),
		Participants: toParticipantDTOs(parts),
	})
}

func (s *Server) handleDeleteTxn(w http.ResponseWriter, r *http.Request) {
	if err := s.coord.DeleteTxn(r.Context(), pathID(r)); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"txn_id": pathID(r), "deleted": true})
}

func (s *Server) handleListParticipants(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	if _, _, err := s.coord.GetTxn(r.Context(), id); err != nil {
		writeError(w, err)
		return
	}
	parts, err := s.coord.ListParticipants(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"txn_id":       id,
		"participants": toParticipantDTOs(parts),
	})
}

// ---- two-phase drive handlers ----------------------------------------------

func (s *Server) handlePrepare(w http.ResponseWriter, r *http.Request) {
	res, err := s.coord.Prepare(r.Context(), pathID(r))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, prepareResponse{
		TxnID:    res.TxnID,
		Decision: res.Decision,
		State:    res.State,
		Votes:    res.Votes,
	})
}

func (s *Server) handleFinish(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	state, parts, err := s.coord.Finish(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, finishResponse{
		TxnID:        id,
		State:        state,
		Participants: toParticipantDTOs(parts),
	})
}

func (s *Server) handleAbort(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	state, parts, err := s.coord.Abort(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, finishResponse{
		TxnID:        id,
		State:        state,
		Participants: toParticipantDTOs(parts),
	})
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	id := pathID(r)
	res, err := s.coord.Run(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, runResponse{
		TxnID:        res.TxnID,
		Decision:     res.Decision,
		State:        res.State,
		Participants: toParticipantDTOs(res.Participants),
	})
}

// ---- decision handlers ------------------------------------------------------

func (s *Server) handleListDecisions(w http.ResponseWriter, r *http.Request) {
	rows, err := s.coord.ListDecisions(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]decisionDTO, 0, len(rows))
	for _, d := range rows {
		out = append(out, decisionDTO{
			TxnID:      d.TxnID,
			Decision:   d.Decision,
			RecordedAt: formatUnixNano(d.RecordedAt),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"decisions": out})
}

func (s *Server) handleGetDecision(w http.ResponseWriter, r *http.Request) {
	id := pathTxnID(r)
	d, ok, err := s.coord.GetDecision(r.Context(), id)
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "decision not found"})
		return
	}
	writeJSON(w, http.StatusOK, decisionDTO{
		TxnID:      d.TxnID,
		Decision:   d.Decision,
		RecordedAt: formatUnixNano(d.RecordedAt),
	})
}

// ---- ledger handlers --------------------------------------------------------

func (s *Server) handleListLedger(w http.ResponseWriter, r *http.Request) {
	rows, err := s.coord.ListLedger(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]ledgerDTO, 0, len(rows))
	for _, l := range rows {
		out = append(out, ledgerDTO{Resource: l.Resource, TxnID: l.TxnID, AppliedAt: formatUnixNano(l.AppliedAt)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"ledger": out})
}

func (s *Server) handleListLedgerForResource(w http.ResponseWriter, r *http.Request) {
	res := pathResource(r)
	rows, ok, err := s.coord.ListLedgerForResource(r.Context(), res)
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "resource not found"})
		return
	}
	out := make([]ledgerDTO, 0, len(rows))
	for _, l := range rows {
		out = append(out, ledgerDTO{Resource: l.Resource, TxnID: l.TxnID, AppliedAt: formatUnixNano(l.AppliedAt)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"resource": res, "ledger": out})
}

// ---- ops & recovery handlers -----------------------------------------------

func (s *Server) handleRecover(w http.ResponseWriter, r *http.Request) {
	recs, err := s.coord.Recover(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]recoveryDTO, 0, len(recs))
	for _, rc := range recs {
		out = append(out, recoveryDTO{TxnID: rc.TxnID, FromState: rc.FromState, ToState: rc.ToState})
	}
	writeJSON(w, http.StatusOK, recoverResponse{Recovered: out})
}

func (s *Server) handleRecoverPreview(w http.ResponseWriter, r *http.Request) {
	rows, err := s.coord.RecoverPreview(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]recoverPreviewDTO, 0, len(rows))
	for _, row := range rows {
		out = append(out, recoverPreviewDTO{
			TxnID:        row.TxnID,
			CurrentState: row.CurrentState,
			TargetState:  row.TargetState,
		})
	}
	writeJSON(w, http.StatusOK, recoverPreviewResponse{Pending: out})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.coord.Stats(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, statsDTO{
		TxnCount:      st.TxnCount,
		ByState:       st.ByState,
		ResourceCount: st.ResourceCount,
		LedgerCount:   st.LedgerCount,
		DecisionCount: st.DecisionCount,
	})
}

// ---- health handlers --------------------------------------------------------

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	// The server only starts listening after startup recovery completes (see
	// main.start), so reaching this handler means recovery is done.
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "recovered": true})
}

// ---- DTO converters ---------------------------------------------------------

func toResourceDTO(r store.ResourceRow) resourceDTO {
	return resourceDTO{
		Name:           r.Name,
		Vote:           r.Vote,
		CommittedCount: r.CommittedCount,
		AbortedCount:   r.AbortedCount,
		CreatedAt:      formatUnixNano(r.CreatedAt),
	}
}

func toParticipantDTOs(parts []store.ParticipantRow) []participantDTO {
	out := make([]participantDTO, 0, len(parts))
	for _, p := range parts {
		out = append(out, participantDTO{
			Resource:    p.Resource,
			Vote:        p.Vote,
			Final:       p.Final,
			VotedAt:     formatUnixNano(p.VotedAt),
			FinalizedAt: formatUnixNano(p.FinalizedAt),
		})
	}
	return out
}

// formatUnixNano renders a stored unix-nanosecond timestamp as an RFC3339
// string. Zero (meaning "not set") renders as "".
func formatUnixNano(ns int64) string {
	if ns == 0 {
		return ""
	}
	return time.Unix(0, ns).UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}
