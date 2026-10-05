package store

import (
	"time"
)

// Message-revision ordering, shared by the tombstone logic. Revisions form
// chains via predecessor event ids; ordering prefers chain descendants, then
// event time, then event id as a deterministic tiebreaker.

type revisionOrder struct {
	eventAt            time.Time
	observedAt         time.Time
	eventID            string
	eventType          string
	predecessorEventID string
}

func revisionDescendsFrom(eventID, ancestorID string, predecessors map[string]string) bool {
	if eventID == "" || ancestorID == "" || eventID == ancestorID {
		return false
	}
	seen := make(map[string]struct{})
	for current := eventID; current != ""; current = predecessors[current] {
		if current == ancestorID {
			return true
		}
		if _, exists := seen[current]; exists {
			return false
		}
		seen[current] = struct{}{}
	}
	return false
}

func preferRevisionOrder(candidate, current revisionOrder, predecessors map[string]string) bool {
	if revisionDescendsFrom(candidate.eventID, current.eventID, predecessors) {
		return true
	}
	if revisionDescendsFrom(current.eventID, candidate.eventID, predecessors) {
		return false
	}
	if comparison := compareRevisionOrder(candidate, current); comparison != 0 {
		return comparison > 0
	}
	return candidate.eventID > current.eventID
}

func compareRevisionOrder(left, right revisionOrder) int {
	if !left.eventAt.Equal(right.eventAt) {
		if left.eventAt.After(right.eventAt) {
			return 1
		}
		return -1
	}
	return 0
}
