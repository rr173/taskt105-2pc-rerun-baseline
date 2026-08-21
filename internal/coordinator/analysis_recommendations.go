package coordinator

func (a CoordinatorAnalysis) Recommendations() []string {
	result := []string{}
	if a.RecoveryPending > 0 {
		result = append(result, "run recovery before accepting new work")
	}
	if a.Votes.Unknown > 0 {
		result = append(result, "inspect transactions that have not reached prepare")
	}
	// Only a commit decision carries a committed ledger effect; an abort
	// decision has no ledger row by design, so it must never be reported as a
	// missing ledger entry. Compare ledger rows against commit decisions only.
	if a.CommitDecisions > a.LedgerRows {
		result = append(result, "review commit decisions without committed ledger effects")
	}
	if len(result) == 0 {
		result = append(result, "no recovery action required")
	}
	return result
}
