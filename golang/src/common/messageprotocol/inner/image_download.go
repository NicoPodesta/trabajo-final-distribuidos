package inner

import (
	"encoding/json"

	"trabajo-final-distribuidos/common/middleware"
)

type ImageDownloadRequest struct {
	Name     string `json:"name"`
	Url      string `json:"url"`
	ImageRef string `json:"image_ref"`
}

// Cada mensaje de datos contiene un pedido. El EOF no contiene un pedido
type ImageDownloadMessage struct {
	Type     MsgType               `json:"type"`
	ClientID string                `json:"client_id"`
	Request  *ImageDownloadRequest `json:"request,omitempty"`
}

func (m *ImageDownloadMessage) IsEOF() bool {
	return m.Type == MsgEOF
}

func (m *ImageDownloadMessage) Serialize() (*middleware.Message, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeImageDownload(clientID string, request ImageDownloadRequest) (*middleware.Message, error) {
	return (&ImageDownloadMessage{
		Type:     MsgData,
		ClientID: clientID,
		Request:  &request,
	}).Serialize()
}

func SerializeImageDownloadsEOF(clientID string) (*middleware.Message, error) {
	return (&ImageDownloadMessage{Type: MsgEOF, ClientID: clientID}).Serialize()
}

func DeserializeImageDownload(message *middleware.Message) (*ImageDownloadMessage, error) {
	var msg ImageDownloadMessage
	if err := json.Unmarshal([]byte(message.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
