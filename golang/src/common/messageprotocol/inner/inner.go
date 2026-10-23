package inner

import (
	"encoding/json"
	"errors"

	"trabajo-final-distribuidos/common/middleware"
	"trabajo-final-distribuidos/common/result"
)

type Q1ArticleItem struct {
	Language     string
	Url          string
	DateModified uint32
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

func SerializeQ1Articles(records []Q1ArticleItem) (*middleware.Message, error) {
	data := []interface{}{}
	for _, record := range records {
		datum := []interface{}{
			record.Language,
			record.Url,
			record.DateModified,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeQ1Articles(message *middleware.Message) ([]Q1ArticleItem, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, false, err
	}

	records := []Q1ArticleItem{}
	for _, datum := range data {
		itemSlice, ok := datum.([]interface{})
		if !ok || len(itemSlice) < 3 {
			return nil, false, errors.New("datum is not an article tuple")
		}

		language, ok := itemSlice[0].(string)
		if !ok {
			return nil, false, errors.New("datum language is not a string")
		}

		url, ok := itemSlice[1].(string)
		if !ok {
			return nil, false, errors.New("datum url is not a string")
		}

		dateModifiedFloat, ok := itemSlice[2].(float64)
		if !ok {
			return nil, false, errors.New("datum date_modified is not a number")
		}

		record := Q1ArticleItem{
			Language:     language,
			Url:          url,
			DateModified: uint32(dateModifiedFloat),
		}
		records = append(records, record)
	}

	return records, len(records) == 0, nil
}

func SerializeQ1Results(records []result.Q1Result) (*middleware.Message, error) {
	data := []interface{}{}
	for _, record := range records {
		datum := []interface{}{
			record.Url,
			record.DateModified,
		}
		data = append(data, datum)
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeQ1Results(message *middleware.Message) ([]result.Q1Result, bool, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, false, err
	}

	records := []result.Q1Result{}
	for _, datum := range data {
		itemSlice, ok := datum.([]interface{})
		if !ok || len(itemSlice) < 2 {
			return nil, false, errors.New("datum is not a result tuple")
		}

		url, ok := itemSlice[0].(string)
		if !ok {
			return nil, false, errors.New("datum url is not a string")
		}

		dateModifiedFloat, ok := itemSlice[1].(float64)
		if !ok {
			return nil, false, errors.New("datum date_modified is not a number")
		}

		record := result.Q1Result{
			Url:          url,
			DateModified: uint32(dateModifiedFloat),
		}
		records = append(records, record)
	}

	return records, len(records) == 0, nil
}
