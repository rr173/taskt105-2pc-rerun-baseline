package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func (a CoordinatorAnalysis) Fingerprint() string {
	raw, err := json.Marshal(a)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
