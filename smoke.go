package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"task105-2pc/internal/coordinator"
	"task105-2pc/internal/httpapi"
	"task105-2pc/internal/store"
)

// runSmokeTest exercises the 2PC coordinator end-to-end through the HTTP
// layer against a temporary SQLite database, including all three restart
// recovery paths (COMMITTING/ABORTING/PREPARING), the explicit abort, run,
// resource-deletion-in-use, decisions, ledger, preview and stats endpoints.
// It returns nil on success.
func runSmokeTest() error {
	dir, err := os.MkdirTemp("", "2pc-smoke-*")
	if err != nil {
		return fmt.Errorf("mkdirtemp: %w", err)
	}
	defer os.RemoveAll(dir)
	dbPath := filepath.Join(dir, "smoke.db")
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := coordinator.NewMockClock(base)

	ts, srv, err := openServer(dbPath, clk)
	if err != nil {
		return err
	}
	defer ts.Close()
	defer srv.store.Close()

	hc := &httpClient{base: ts.URL}

	// ---- healthz / ready ----
	if _, code, err := hc.get("/healthz"); err != nil || code != 200 {
		return fmt.Errorf("healthz: code=%d err=%v", code, err)
	}
	if _, code, err := hc.get("/healthz/ready"); err != nil || code != 200 {
		return fmt.Errorf("ready: code=%d err=%v", code, err)
	}

	// ---- register resources (R1,R2 yes; R3 no; R4 default vote) ----
	for _, r := range []struct{ name, vote string }{{"R1", "yes"}, {"R2", "yes"}, {"R3", "no"}, {"R4", ""}} {
		body, code, err := hc.post("/resources", map[string]any{"name": r.name, "vote": r.vote})
		if err != nil || code != 200 {
			return fmt.Errorf("register %s: code=%d err=%v body=%v", r.name, code, err, body)
		}
		if r.name == "R4" && body["vote"] != "yes" {
			return fmt.Errorf("R4 default vote wrong: %v", body)
		}
		if body["committed_count"].(float64) != 0 || body["aborted_count"].(float64) != 0 {
			return fmt.Errorf("R1 counters not zero: %v", body)
		}
	}
	// duplicate resource -> 409
	if _, code, _ := hc.post("/resources", map[string]any{"name": "R1", "vote": "yes"}); code != 409 {
		return fmt.Errorf("duplicate resource: code=%d", code)
	}
	// bad vote -> 400
	if _, code, _ := hc.post("/resources", map[string]any{"name": "RX", "vote": "maybe"}); code != 400 {
		return fmt.Errorf("bad vote: code=%d", code)
	}
	// list resources -> 4, ordered by name
	if body, code, _ := hc.get("/resources"); code != 200 || len(body["resources"].([]any)) != 4 {
		return fmt.Errorf("list resources: code=%d body=%v", code, body)
	}
	// get single resource
	if body, code, _ := hc.get("/resources/R3"); code != 200 || body["vote"] != "no" {
		return fmt.Errorf("get R3: code=%d body=%v", code, body)
	}
	if _, code, _ := hc.get("/resources/NOPE"); code != 404 {
		return fmt.Errorf("get missing resource: code=%d", code)
	}
	// update resource vote R3 no->yes
	if body, code, _ := hc.put("/resources/R3", map[string]any{"vote": "yes"}); code != 200 || body["vote"] != "yes" {
		return fmt.Errorf("update R3 vote: code=%d body=%v", code, body)
	}
	if _, code, _ := hc.put("/resources/NOPE", map[string]any{"vote": "yes"}); code != 404 {
		return fmt.Errorf("update missing resource: code=%d", code)
	}

	// ---- commit path via run: T1 = [R1,R2], both yes ----
	if body, code, err := hc.post("/txns", map[string]any{"txn_id": "T1", "resources": []string{"R1", "R2"}}); err != nil || code != 200 || body["state"] != "PREPARING" {
		return fmt.Errorf("begin T1: code=%d body=%v", code, body)
	}
	if _, code, _ := hc.post("/txns", map[string]any{"txn_id": "T1", "resources": []string{"R1"}}); code != 409 {
		return fmt.Errorf("duplicate txn: code=%d", code)
	}
	if _, code, _ := hc.post("/txns", map[string]any{"txn_id": "Tbad", "resources": []string{"NOPE"}}); code != 400 {
		return fmt.Errorf("unknown resource: code=%d", code)
	}
	run1, code, err := hc.post("/txns/T1/run", nil)
	if err != nil || code != 200 || run1["decision"] != "commit" || run1["state"] != "COMMITTED" {
		return fmt.Errorf("run T1: code=%d body=%v", code, run1)
	}
	// resource counters: R1.committed_count=1, R2.committed_count=1
	if body, _, _ := hc.get("/resources/R1"); body["committed_count"].(float64) != 1 {
		return fmt.Errorf("R1 committed_count after T1: %v", body)
	}
	if body, _, _ := hc.get("/resources/R2"); body["committed_count"].(float64) != 1 {
		return fmt.Errorf("R2 committed_count after T1: %v", body)
	}
	// ledger has 2 entries for T1
	if n, _ := srv.coord.LedgerCount(context.Background(), "T1"); n != 2 {
		return fmt.Errorf("T1 ledger count=%d want 2", n)
	}
	// run again -> 409 (terminal)
	if _, code, _ := hc.post("/txns/T1/run", nil); code != 409 {
		return fmt.Errorf("run terminal T1: code=%d", code)
	}
	// prepare on terminal -> 409
	if _, code, _ := hc.post("/txns/T1/prepare", nil); code != 409 {
		return fmt.Errorf("prepare terminal T1: code=%d", code)
	}

	// ---- abort path: T2 = [R1,R3], R3 votes yes now (updated) so commit.
	// Use R5=no to force an abort. ----
	hc.post("/resources", map[string]any{"name": "R5", "vote": "no"})
	if body, code, _ := hc.post("/txns", map[string]any{"txn_id": "T2", "resources": []string{"R1", "R5"}}); code != 200 {
		return fmt.Errorf("begin T2: code=%d body=%v", code, body)
	}
	pr2, code, err := hc.post("/txns/T2/prepare", nil)
	if err != nil || code != 200 || pr2["decision"] != "abort" || pr2["state"] != "ABORTING" {
		return fmt.Errorf("prepare T2: code=%d body=%v", code, pr2)
	}
	fr2, code, err := hc.post("/txns/T2/finish", nil)
	if err != nil || code != 200 || fr2["state"] != "ABORTED" {
		return fmt.Errorf("finish T2: code=%d body=%v", code, fr2)
	}
	// abort consistency: all participants final=aborted
	for _, p := range fr2["participants"].([]any) {
		if p.(map[string]any)["final"] != "aborted" {
			return fmt.Errorf("T2 mixed final: %v", p)
		}
	}
	// aborted counters: R1.aborted_count=1, R5.aborted_count=1; R1 committed unchanged
	if body, _, _ := hc.get("/resources/R1"); body["aborted_count"].(float64) != 1 || body["committed_count"].(float64) != 1 {
		return fmt.Errorf("R1 counters after T2: %v", body)
	}
	if body, _, _ := hc.get("/resources/R5"); body["aborted_count"].(float64) != 1 {
		return fmt.Errorf("R5 aborted_count after T2: %v", body)
	}
	if n, _ := srv.coord.LedgerCount(context.Background(), "T2"); n != 0 {
		return fmt.Errorf("T2 ledger count=%d want 0", n)
	}

	// ---- explicit abort on PREPARING: T3 = [R1,R2] ----
	hc.post("/txns", map[string]any{"txn_id": "T3", "resources": []string{"R1", "R2"}})
	ab3, code, err := hc.post("/txns/T3/abort", nil)
	if err != nil || code != 200 || ab3["state"] != "ABORTED" {
		return fmt.Errorf("abort T3: code=%d body=%v", code, ab3)
	}
	if body, _, _ := hc.get("/resources/R1"); body["aborted_count"].(float64) != 2 {
		return fmt.Errorf("R1 aborted_count after T3: %v", body)
	}
	// abort on a COMMITTING txn -> 409 (cannot overturn commit decision)
	hc.post("/txns", map[string]any{"txn_id": "TabortCommit", "resources": []string{"R1", "R2"}})
	hc.post("/txns/TabortCommit/prepare", nil) // -> COMMITTING
	if _, code, _ := hc.post("/txns/TabortCommit/abort", nil); code != 409 {
		return fmt.Errorf("abort on COMMITTING: code=%d want 409", code)
	}
	hc.post("/txns/TabortCommit/finish", nil) // clean up to COMMITTED

	// ---- finish a PREPARING txn (no decision) -> 409 PREPARING ----
	hc.post("/txns", map[string]any{"txn_id": "Tprep", "resources": []string{"R1"}})
	if body, code, _ := hc.post("/txns/Tprep/finish", nil); code != 409 || body["error"] != "PREPARING" {
		return fmt.Errorf("finish PREPARING: code=%d body=%v", code, body)
	}

	// ---- GET /txns/{id}, /txns, /txns/{id}/participants ----
	if gt, code, _ := hc.get("/txns/T1"); code != 200 || gt["state"] != "COMMITTED" {
		return fmt.Errorf("get T1: code=%d body=%v", code, gt)
	}
	if _, code, _ := hc.get("/txns/missing"); code != 404 {
		return fmt.Errorf("get missing txn: code=%d", code)
	}
	if lt, code, _ := hc.get("/txns"); code != 200 || len(lt["txns"].([]any)) < 4 {
		return fmt.Errorf("list txns: code=%d body=%v", code, lt)
	}
	// state filter
	if lt, _, _ := hc.get("/txns?state=COMMITTED"); len(lt["txns"].([]any)) < 2 {
		return fmt.Errorf("list committed txns: %v", lt)
	}
	if pt, code, _ := hc.get("/txns/T1/participants"); code != 200 || len(pt["participants"].([]any)) != 2 {
		return fmt.Errorf("list T1 participants: code=%d body=%v", code, pt)
	}
	if _, code, _ := hc.get("/txns/missing/participants"); code != 404 {
		return fmt.Errorf("participants missing txn: code=%d", code)
	}

	// ---- decisions endpoints ----
	if dd, code, _ := hc.get("/decisions"); code != 200 || len(dd["decisions"].([]any)) < 3 {
		return fmt.Errorf("list decisions: code=%d body=%v", code, dd)
	}
	if d, code, _ := hc.get("/decisions/T1"); code != 200 || d["decision"] != "commit" {
		return fmt.Errorf("get decision T1: code=%d body=%v", code, d)
	}
	if _, code, _ := hc.get("/decisions/Tprep"); code != 404 {
		return fmt.Errorf("get decision Tprep (PREPARING, none): code=%d want 404", code)
	}

	// ---- ledger endpoints ----
	if lg, code, _ := hc.get("/ledger"); code != 200 || len(lg["ledger"].([]any)) < 2 {
		return fmt.Errorf("list ledger: code=%d body=%v", code, lg)
	}
	if lgr, code, _ := hc.get("/ledger/R1"); code != 200 || len(lgr["ledger"].([]any)) < 2 {
		return fmt.Errorf("ledger R1: code=%d body=%v", code, lgr)
	}
	if _, code, _ := hc.get("/ledger/NOPE"); code != 404 {
		return fmt.Errorf("ledger missing resource: code=%d", code)
	}

	// ---- stats ----
	if st, code, _ := hc.get("/admin/stats"); code != 200 || st["resources"].(float64) < 4 {
		return fmt.Errorf("stats: code=%d body=%v", code, st)
	}

	// ---- resource in use: try to delete R1 while Tprep (PREPARING) refs it ----
	if body, code, _ := hc.del("/resources/R1"); code != 409 || body["error"] != "resource in use" {
		return fmt.Errorf("delete R1 in use: code=%d body=%v", code, body)
	}
	// Tprep only exists to prove the resource-in-use guard. Finish that
	// temporary transaction before the restart-recovery scenario so its abort
	// is not mixed into the counters being measured below.
	if body, code, _ := hc.post("/txns/Tprep/abort", nil); code != 200 || body["state"] != "ABORTED" {
		return fmt.Errorf("abort temporary Tprep: code=%d body=%v", code, body)
	}
	if body, code, _ := hc.del("/txns/Tprep"); code != 200 || body["deleted"] != true {
		return fmt.Errorf("delete temporary Tprep: code=%d body=%v", code, body)
	}

	// ---- restart recovery: set up 3 in-doubt txns, crash, reopen ----
	// T-rec-c (COMMITTING): prepare only
	hc.post("/txns", map[string]any{"txn_id": "TrecC", "resources": []string{"R1", "R2"}})
	hc.post("/txns/TrecC/prepare", nil)
	// T-rec-a (ABORTING): prepare with R5(no) -> abort
	hc.post("/txns", map[string]any{"txn_id": "TrecA", "resources": []string{"R1", "R5"}})
	hc.post("/txns/TrecA/prepare", nil)
	// T-rec-p (PREPARING): begin only, never prepared
	hc.post("/txns", map[string]any{"txn_id": "TrecP", "resources": []string{"R1", "R2"}})

	// preview before crash
	pv, code, err := hc.get("/admin/recover/preview")
	if err != nil || code != 200 || len(pv["pending"].([]any)) < 3 {
		return fmt.Errorf("recover preview: code=%d body=%v", code, pv)
	}

	// snapshot R1 counters before crash
	r1Before, _, _ := hc.get("/resources/R1")
	r1CommittedBefore := r1Before["committed_count"].(float64)
	r1AbortedBefore := r1Before["aborted_count"].(float64)

	// Simulate crash: close store + http server, reopen same DB.
	ts.Close()
	srv.store.Close()
	ts2, srv2, err := openServer(dbPath, clk)
	if err != nil {
		return err
	}
	defer ts2.Close()
	defer srv2.store.Close()
	hc2 := &httpClient{base: ts2.URL}

	// startup recovery already ran in openServer; verify terminal states.
	if g, code, _ := hc2.get("/txns/TrecC"); code != 200 || g["state"] != "COMMITTED" {
		return fmt.Errorf("recover TrecC (COMMITTING->COMMITTED): code=%d body=%v", code, g)
	}
	if n, _ := srv2.coord.LedgerCount(context.Background(), "TrecC"); n != 2 {
		return fmt.Errorf("TrecC post-recover ledger=%d want 2", n)
	}
	if g, code, _ := hc2.get("/txns/TrecA"); code != 200 || g["state"] != "ABORTED" {
		return fmt.Errorf("recover TrecA (ABORTING->ABORTED): code=%d body=%v", code, g)
	}
	if n, _ := srv2.coord.LedgerCount(context.Background(), "TrecA"); n != 0 {
		return fmt.Errorf("TrecA post-recover ledger=%d want 0", n)
	}
	// TrecP was PREPARING: recovery MUST abort (not commit), even though R1/R2 vote yes.
	if g, code, _ := hc2.get("/txns/TrecP"); code != 200 || g["state"] != "ABORTED" {
		return fmt.Errorf("recover TrecP (PREPARING->ABORTED): code=%d body=%v", code, g)
	}
	if trecP, code, _ := hc2.get("/txns/TrecP"); code != 200 || trecP["decision"] != "abort" {
		return fmt.Errorf("TrecP decision=%v want abort", trecP["decision"])
	}
	if n, _ := srv2.coord.LedgerCount(context.Background(), "TrecP"); n != 0 {
		return fmt.Errorf("TrecP post-recover ledger=%d want 0", n)
	}

	// Counters must reflect recovery exactly once per effect (idempotent):
	// R1 gained 1 committed (TrecC) + 2 aborted (TrecA, TrecP) over the crash.
	r1After, _, _ := hc2.get("/resources/R1")
	if r1After["committed_count"].(float64) != r1CommittedBefore+1 {
		return fmt.Errorf("R1 committed_count recovery not exactly once: before=%v after=%v", r1CommittedBefore, r1After["committed_count"])
	}
	if r1After["aborted_count"].(float64) != r1AbortedBefore+2 {
		return fmt.Errorf("R1 aborted_count recovery not exactly once: before=%v after=%v", r1AbortedBefore, r1After["aborted_count"])
	}

	// ---- idempotent recovery: re-running recover changes nothing ----
	recBody, code, err := hc2.post("/admin/recover", nil)
	if err != nil || code != 200 {
		return fmt.Errorf("recover idempotent: code=%d body=%v", code, recBody)
	}
	if recs, ok := recBody["recovered"].([]any); ok && len(recs) != 0 {
		return fmt.Errorf("idempotent recover found work: %v", recs)
	}
	r1ReRecover, _, _ := hc2.get("/resources/R1")
	if r1ReRecover["committed_count"].(float64) != r1CommittedBefore+1 ||
		r1ReRecover["aborted_count"].(float64) != r1AbortedBefore+2 {
		return fmt.Errorf("re-recover changed counters: %v", r1ReRecover)
	}

	// ---- direct recover endpoint drives an in-doubt txn ----
	hc2.post("/txns", map[string]any{"txn_id": "Trec6", "resources": []string{"R1", "R2"}})
	hc2.post("/txns/Trec6/prepare", nil)
	rec6, code, err := hc2.post("/admin/recover", nil)
	if err != nil || code != 200 || len(rec6["recovered"].([]any)) != 1 {
		return fmt.Errorf("recover Trec6: code=%d body=%v", code, rec6)
	}
	rm := rec6["recovered"].([]any)[0].(map[string]any)
	if rm["txn_id"] != "Trec6" || rm["from_state"] != "COMMITTING" || rm["to_state"] != "COMMITTED" {
		return fmt.Errorf("recover Trec6 record: %v", rm)
	}
	r1AfterTrec6, _, _ := hc2.get("/resources/R1")

	// ---- delete a terminal txn (cleanup) ----
	if body, code, _ := hc2.del("/txns/Trec6"); code != 200 || body["deleted"] != true {
		return fmt.Errorf("delete Trec6: code=%d body=%v", code, body)
	}
	if _, code, _ := hc2.get("/txns/Trec6"); code != 404 {
		return fmt.Errorf("Trec6 still present after delete: code=%d", code)
	}
	// delete a non-terminal txn -> 409
	hc2.post("/txns", map[string]any{"txn_id": "Tnonterm", "resources": []string{"R1"}})
	if _, code, _ := hc2.del("/txns/Tnonterm"); code != 409 {
		return fmt.Errorf("delete non-terminal: code=%d want 409", code)
	}
	// delete a missing txn -> 404
	if _, code, _ := hc2.del("/txns/does-not-exist"); code != 404 {
		return fmt.Errorf("delete missing txn: code=%d want 404", code)
	}
	// cleanup did NOT change resource counters (counters are cumulative history)
	r1AfterClean, _, _ := hc2.get("/resources/R1")
	if r1AfterClean["committed_count"].(float64) != r1AfterTrec6["committed_count"].(float64) {
		return fmt.Errorf("cleanup changed committed_count: %v -> %v", r1AfterTrec6, r1AfterClean)
	}

	// ---- now that all txns are terminal, R1 can be deleted ----
	// First finish Tnonterm (or it blocks). Use recover.
	hc2.post("/admin/recover", nil)
	if body, code, _ := hc2.del("/resources/R1"); code != 200 || body["deleted"] != true {
		return fmt.Errorf("delete R1 after drain: code=%d body=%v", code, body)
	}
	if _, code, _ := hc2.get("/resources/R1"); code != 404 {
		return fmt.Errorf("R1 still present after delete: code=%d", code)
	}

	// ---- method not allowed -> 405 ----
	req, _ := http.NewRequest("PUT", ts2.URL+"/txns", &bytes.Buffer{})
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 405 {
		return fmt.Errorf("PUT /txns: status=%v err=%v", resp.StatusCode, err)
	}
	resp.Body.Close()

	return nil
}

// openServer opens the store at dbPath with the given clock, wires the
// coordinator, runs startup recovery, and returns a running httptest server.
func openServer(dbPath string, clk *coordinator.MockClock) (*httptest.Server, *assembledServer, error) {
	st, err := store.Open(dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open store: %w", err)
	}
	coord, err := coordinator.New(st, clk)
	if err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("new coordinator: %w", err)
	}
	if _, err := coord.Recover(context.Background()); err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("startup recover: %w", err)
	}
	srv := httpapi.New(coord)
	ts := httptest.NewServer(httpapi.NewRouter(srv))
	return ts, &assembledServer{store: st, coord: coord}, nil
}

// httpClient is a small JSON HTTP client used by the smoke test.
type httpClient struct{ base string }

func (c *httpClient) post(path string, body any) (map[string]any, int, error) {
	return c.do("POST", path, body)
}
func (c *httpClient) put(path string, body any) (map[string]any, int, error) {
	return c.do("PUT", path, body)
}
func (c *httpClient) del(path string) (map[string]any, int, error) {
	return c.do("DELETE", path, nil)
}
func (c *httpClient) get(path string) (map[string]any, int, error) {
	return c.do("GET", path, nil)
}

func (c *httpClient) do(method, path string, body any) (map[string]any, int, error) {
	var buf bytes.Buffer
	if body != nil {
		raw, _ := json.Marshal(body)
		buf.Write(raw)
	}
	req, err := http.NewRequest(method, c.base+path, &buf)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out, resp.StatusCode, nil
}
