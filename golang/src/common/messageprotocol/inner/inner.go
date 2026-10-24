package inner

import (
	"encoding/json"
	"errors"
	"strings"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q1ArticleItem struct {
	Language     string
	Url          string
	DateModified uint32
}

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

func SerializeQ1Articles(clientID string, records []Q1ArticleItem) (*middleware.Message, error) {
	recordsData := []interface{}{}
	for _, record := range records {
		item := []interface{}{
			record.Language,
			record.Url,
			record.DateModified,
		}
		recordsData = append(recordsData, item)
	}

	body, err := serializeJson([]interface{}{clientID, recordsData})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeQ1Articles(message *middleware.Message) (string, []Q1ArticleItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, err
	}

	if len(data) < 2 {
		return "", nil, false, errors.New("message does not contain clientID and records")
	}

	clientID, ok := data[0].(string)
	if !ok {
		return "", nil, false, errors.New("clientID is not a string")
	}

	recordsRaw, ok := data[1].([]interface{})
	if !ok {
		return "", nil, false, errors.New("records is not a list")
	}

	records := make([]Q1ArticleItem, 0, len(recordsRaw))
	for _, item := range recordsRaw {
		itemSlice, ok := item.([]interface{})
		if !ok || len(itemSlice) < 3 {
			return "", nil, false, errors.New("item is not an article tuple")
		}

		language, ok := itemSlice[0].(string)
		if !ok {
			return "", nil, false, errors.New("item language is not a string")
		}

		url, ok := itemSlice[1].(string)
		if !ok {
			return "", nil, false, errors.New("item url is not a string")
		}

		dateModifiedFloat, ok := itemSlice[2].(float64)
		if !ok {
			return "", nil, false, errors.New("item date_modified is not a number")
		}

		record := Q1ArticleItem{
			Language:     language,
			Url:          url,
			DateModified: uint32(dateModifiedFloat),
		}
		records = append(records, record)
	}

	return clientID, records, len(records) == 0, nil
}

func SerializeQ1Results(clientID string, records []result.Q1Result) (*middleware.Message, error) {
	recordsData := []interface{}{}
	for _, record := range records {
		item := []interface{}{
			record.Url,
			record.DateModified,
		}
		recordsData = append(recordsData, item)
	}

	body, err := serializeJson([]interface{}{clientID, recordsData})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeQ1Results(message *middleware.Message) (string, []result.Q1Result, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return "", nil, false, err
	}

	if len(data) < 2 {
		return "", nil, false, errors.New("message does not contain clientID and results")
	}

	clientID, ok := data[0].(string)
	if !ok {
		return "", nil, false, errors.New("clientID is not a string")
	}

	recordsRaw, ok := data[1].([]interface{})
	if !ok {
		return "", nil, false, errors.New("records is not a list")
	}

	records := make([]result.Q1Result, 0, len(recordsRaw))
	for _, item := range recordsRaw {
		itemSlice, ok := item.([]interface{})
		if !ok || len(itemSlice) < 2 {
			return "", nil, false, errors.New("item is not a result tuple")
		}

		url, ok := itemSlice[0].(string)
		if !ok {
			return "", nil, false, errors.New("item url is not a string")
		}

		dateModifiedFloat, ok := itemSlice[1].(float64)
		if !ok {
			return "", nil, false, errors.New("item date_modified is not a number")
		}

		record := result.Q1Result{
			Url:          url,
			DateModified: uint32(dateModifiedFloat),
		}
		records = append(records, record)
	}

	return clientID, records, len(records) == 0, nil
}
