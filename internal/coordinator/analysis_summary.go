package coordinator

type AnalysisSummary struct {
	Transactions    int  `json:"transactions"`
	Resources       int  `json:"resources"`
	Decisions       int  `json:"decisions"`
	CommitDecisions int  `json:"commit_decisions"`
	LedgerRows      int  `json:"ledger_rows"`
	Pending         int  `json:"pending"`
	Healthy         bool `json:"healthy"`
}

func (a CoordinatorAnalysis) Summary() AnalysisSummary {
	return AnalysisSummary{Transactions: len(a.Transactions), Resources: len(a.Resources), Decisions: a.Decisions, CommitDecisions: a.CommitDecisions, LedgerRows: a.LedgerRows, Pending: a.RecoveryPending, Healthy: a.Healthy}
}
