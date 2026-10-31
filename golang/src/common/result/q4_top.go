package result

import "sort"

const Q4TopSize = 20

func TopQ4(records []Q4Result) []Q4Result {
	sort.Slice(records, func(i, j int) bool {
		if records[i].Count != records[j].Count {
			return records[i].Count > records[j].Count
		}
		return records[i].Word < records[j].Word
	})
	return records[:min(Q4TopSize, len(records))]
}
