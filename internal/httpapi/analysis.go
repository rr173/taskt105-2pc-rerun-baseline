package httpapi

import (
	"net/http"
)

func (s *Server) handleAnalysis(w http.ResponseWriter, r *http.Request) {
	analysis, err := s.coord.Analyze(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"analysis": analysis, "summary": analysis.Summary(), "recommendations": analysis.Recommendations(), "fingerprint": analysis.Fingerprint()})
}
