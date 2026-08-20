package coordinator

func validDiagnostic(txn TxnDiagnostic) bool {
	if txn.Participants < 0 || txn.Finalized < 0 || txn.Finalized > txn.Participants {
		return false
	}
	if txn.Completion < 0 || txn.Completion > 100 {
		return false
	}
	return txn.TxnID != ""
}
