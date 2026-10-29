package inner

import (
	"fmt"

	"trabajo-final-distribuidos/common/middleware"
)

const Q5AverageExchange = "q5_average"

func Q5FilterRoutingKey(filterID int) string {
	return fmt.Sprintf("q5_filter_%d", filterID)
}

type AverageMessage = RecordsMessage[uint64]

func SerializeAverage(clientID string, average uint64) (*middleware.Message, error) {
	return serializeRecords(clientID, []uint64{average})
}
