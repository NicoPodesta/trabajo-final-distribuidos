package inner

import (
	"fmt"

	"trabajo-final-distribuidos/common/middleware"
)

const Q5AverageExchange = "q5_average"

func Q5FilterRoutingKey(filterID int) string {
	return fmt.Sprintf("q5_filter_%d", filterID)
}

func Q5FilterRoutingKeys(filterAmount int) []string {
	keys := make([]string, filterAmount)
	for filterID := range keys {
		keys[filterID] = Q5FilterRoutingKey(filterID)
	}
	return keys
}

type AverageMessage = RecordsMessage[uint64]

func SerializeAverage(clientID string, average uint64) (*middleware.Message, error) {
	return serializeRecords(clientID, []uint64{average})
}

func DeserializeAverage(message *middleware.Message) (*AverageMessage, error) {
	return deserializeRecords[uint64](message)
}
