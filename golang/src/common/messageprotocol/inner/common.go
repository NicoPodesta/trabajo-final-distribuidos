package inner

import (
	"encoding/json"
	"strings"
)

func ExtractGatewayID(clientID string) string {
	idx := strings.LastIndex(clientID, "_")
	if idx > 0 {
		return clientID[:idx]
	}
	return clientID
}

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}
