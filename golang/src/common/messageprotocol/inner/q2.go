package inner

import (
	"errors"

	"trabajo-final-distribuidos/common/middleware"
)

type Q2ArticleItem struct {
	Language     string
	Name         string
	Url          string
	DateModified uint32
	Image        string
	Sections     []string
}

func SerializeQ2Articles(clientID string, records []Q2ArticleItem) (*middleware.Message, error) {
	recordsData := []interface{}{}
	for _, record := range records {
		item := []interface{}{
			record.Language,
			record.Name,
			record.Url,
			record.DateModified,
			record.Image,
			record.Sections,
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

func DeserializeQ2Articles(message *middleware.Message) (string, []Q2ArticleItem, bool, error) {
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

	records := make([]Q2ArticleItem, 0, len(recordsRaw))
	for _, item := range recordsRaw {
		itemSlice, ok := item.([]interface{})
		if !ok || len(itemSlice) < 6 {
			return "", nil, false, errors.New("item is not a Q2 article tuple")
		}

		language, ok := itemSlice[0].(string)
		if !ok {
			return "", nil, false, errors.New("item language is not a string")
		}

		name, ok := itemSlice[1].(string)
		if !ok {
			return "", nil, false, errors.New("item name is not a string")
		}

		url, ok := itemSlice[2].(string)
		if !ok {
			return "", nil, false, errors.New("item url is not a string")
		}

		dateModifiedFloat, ok := itemSlice[3].(float64)
		if !ok {
			return "", nil, false, errors.New("item date_modified is not a number")
		}

		image, ok := itemSlice[4].(string)
		if !ok {
			return "", nil, false, errors.New("item image is not a string")
		}

		sectionsRaw, ok := itemSlice[5].([]interface{})
		if !ok {
			return "", nil, false, errors.New("item sections is not a list")
		}

		sections := make([]string, 0, len(sectionsRaw))
		for _, secRaw := range sectionsRaw {
			sec, ok := secRaw.(string)
			if !ok {
				return "", nil, false, errors.New("section name is not a string")
			}
			sections = append(sections, sec)
		}

		record := Q2ArticleItem{
			Language:     language,
			Name:         name,
			Url:          url,
			DateModified: uint32(dateModifiedFloat),
			Image:        image,
			Sections:     sections,
		}
		records = append(records, record)
	}

	return clientID, records, len(records) == 0, nil
}

