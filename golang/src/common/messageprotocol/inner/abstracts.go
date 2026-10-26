package inner

import (
	"errors"

	"trabajo-final-distribuidos/common/middleware"
)

type AbstractItem struct {
	Language     string
	Url          string
	DateModified uint32
	Abstract     string
}

func SerializeAbstracts(clientID string, records []AbstractItem) (*middleware.Message, error) {
	recordsData := []interface{}{}
	for _, record := range records {
		item := []interface{}{
			record.Language,
			record.Url,
			record.DateModified,
			record.Abstract,
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

func DeserializeAbstracts(message *middleware.Message) (string, []AbstractItem, bool, error) {
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

	records := make([]AbstractItem, 0, len(recordsRaw))
	for _, item := range recordsRaw {
		itemSlice, ok := item.([]interface{})
		if !ok || len(itemSlice) < 4 {
			return "", nil, false, errors.New("item is not an abstract tuple")
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

		abstract, ok := itemSlice[3].(string)
		if !ok {
			return "", nil, false, errors.New("item abstract is not a string")
		}

		record := AbstractItem{
			Language:     language,
			Url:          url,
			DateModified: uint32(dateModifiedFloat),
			Abstract:     abstract,
		}
		records = append(records, record)
	}

	return clientID, records, len(records) == 0, nil
}
