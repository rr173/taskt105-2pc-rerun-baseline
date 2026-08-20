package coordinator

import (
	"context"
	"sort"
)

// Analyze returns a read-only view of the durable 2PC decision and effect
// logs. It is intended for recovery checks and operator diagnostics.
func (c *Coordinator) Analyze(ctx context.Context) (CoordinatorAnalysis, error) {
	txns, err := c.transactionDiagnostics(ctx)
	if err != nil {
		return CoordinatorAnalysis{}, err
	}
	resources, err := c.resourceDiagnostics(ctx)
	if err != nil {
		return CoordinatorAnalysis{}, err
	}
	decisions, err := c.decisionCount(ctx)
	if err != nil {
		return CoordinatorAnalysis{}, err
	}
	ledger, err := c.ledgerCount(ctx)
	if err != nil {
		return CoordinatorAnalysis{}, err
	}
	pending, err := c.recoveryCount(ctx)
	if err != nil {
		return CoordinatorAnalysis{}, err
	}
	allParts := VoteSummary{}
	for _, txn := range txns {
		parts, err := c.ListParticipants(ctx, txn.TxnID)
		if err != nil {
			return CoordinatorAnalysis{}, err
		}
		votes := participantVotes(parts)
		allParts.Yes += votes.Yes
		allParts.No += votes.No
		allParts.Unknown += votes.Unknown
	}
	sort.Slice(txns, func(i, j int) bool { return txns[i].TxnID < txns[j].TxnID })
	sort.Slice(resources, func(i, j int) bool { return resources[i].Name < resources[j].Name })
	return CoordinatorAnalysis{Transactions: txns, Resources: resources, Votes: allParts, Decisions: decisions, LedgerRows: ledger, RecoveryPending: pending, Healthy: analysisHealthy(txns, resources, pending)}, nil
}
