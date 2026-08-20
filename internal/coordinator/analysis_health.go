package coordinator

func analysisHealthy(txns []TxnDiagnostic, resources []ResourceDiagnostic, pending int) bool {
	for _, txn := range txns {
		if !validDiagnostic(txn) {
			return false
		}
	}
	for _, resource := range resources {
		if resource.Name == "" || resource.Committed < 0 || resource.Aborted < 0 {
			return false
		}
	}
	return pending == 0
}
