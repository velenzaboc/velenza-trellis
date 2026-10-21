package main

const maxSnapshotUnnestIDs = 1000

func snapshotUnnestIDsLimit() int {
	return maxSnapshotUnnestIDs
}

func chunkSnapshotIDs[T any](ids []T, maxPerChunk int) [][]T {
	if len(ids) == 0 {
		return nil
	}
	if maxPerChunk <= 0 {
		maxPerChunk = maxSnapshotUnnestIDs
	}
	out := make([][]T, 0, (len(ids)+maxPerChunk-1)/maxPerChunk)
	for i := 0; i < len(ids); i += maxPerChunk {
		end := i + maxPerChunk
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[i:end])
	}
	return out
}
