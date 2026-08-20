package coordinator

func (a CoordinatorAnalysis) Recommendations() []string {
	result := []string{}
	if a.RecoveryPending > 0 {
		result = append(result, "run recovery before accepting new work")
	}
	if a.Votes.Unknown > 0 {
		result = append(result, "inspect transactions that have not reached prepare")
	}
	if a.Decisions > a.LedgerRows {
		result = append(result, "review decisions without committed ledger effects")
	}
	if len(result) == 0 {
		result = append(result, "no recovery action required")
	}
	return result
}
