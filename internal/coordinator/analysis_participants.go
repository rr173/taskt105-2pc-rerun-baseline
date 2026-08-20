package coordinator

import "task105-2pc/internal/store"

func participantVotes(parts []store.ParticipantRow) VoteSummary {
	result := VoteSummary{}
	for _, part := range parts {
		switch part.Vote {
		case store.VoteYes:
			result.Yes++
		case store.VoteNo:
			result.No++
		default:
			result.Unknown++
		}
	}
	return result
}
