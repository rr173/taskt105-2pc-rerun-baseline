package coordinator

type TxnDiagnostic struct {
	TxnID        string `json:"txn_id"`
	State        string `json:"state"`
	Decision     string `json:"decision"`
	Participants int    `json:"participants"`
	Finalized    int    `json:"finalized"`
	Completion   int    `json:"completion"`
}
type ResourceDiagnostic struct {
	Name      string `json:"name"`
	Vote      string `json:"vote"`
	Committed int64  `json:"committed"`
	Aborted   int64  `json:"aborted"`
}
type VoteSummary struct {
	Yes     int `json:"yes"`
	No      int `json:"no"`
	Unknown int `json:"unknown"`
}
type CoordinatorAnalysis struct {
	Transactions    []TxnDiagnostic      `json:"transactions"`
	Resources       []ResourceDiagnostic `json:"resources"`
	Votes           VoteSummary          `json:"votes"`
	Decisions       int                  `json:"decisions"`
	LedgerRows      int                  `json:"ledger_rows"`
	RecoveryPending int                  `json:"recovery_pending"`
	Healthy         bool                 `json:"healthy"`
}
