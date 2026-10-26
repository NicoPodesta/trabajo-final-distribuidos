package external

import (
	"fmt"
	"io"

	"trabajo-final-distribuidos/common/article"
	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
	"trabajo-final-distribuidos/common/result"
)

type Reader struct {
	r io.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) readUint32() (uint32, error) {
	serialized, err := safeio.ReadAll(r.r, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeUint32(serialized), nil
}

func (r *Reader) readString() (string, error) {
	serialized, err := r.readBytes()
	if err != nil {
		return "", err
	}
	return serializer.DeserializeString(serialized), nil
}

func (r *Reader) readBytes() ([]byte, error) {
	size, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	return safeio.ReadAll(r.r, size)
}

func (r *Reader) ReadMsgType() (MsgType, error) {
	msgType, err := r.readUint32()
	if err != nil {
		return 0, err
	}
	return MsgType(msgType), nil
}

func (r *Reader) ReadAck() error {
	msgType, err := r.ReadMsgType()
	if err != nil {
		return err
	}
	if msgType != Ack {
		return fmt.Errorf("unexpected message type: %d, expected: %d", msgType, Ack)
	}
	return nil
}

func (r *Reader) ReadEndOfRecords() error {
	msgType, err := r.ReadMsgType()
	if err != nil {
		return err
	}
	if msgType != EndOfRecords {
		return fmt.Errorf("unexpected message type: %d, expected: %d", msgType, EndOfRecords)
	}
	return nil
}

func (r *Reader) ReadGatewayAssignment() (string, error) {
	msgType, err := r.ReadMsgType()
	if err != nil {
		return "", err
	}
	if msgType != GatewayAssignment {
		return "", fmt.Errorf("unexpected message type: %d, expected: %d", msgType, GatewayAssignment)
	}
	return r.readString()
}

func (r *Reader) ReadArticleRecord() (*article.Article, error) {
	language, err := r.readString()
	if err != nil {
		return nil, err
	}

	name, err := r.readString()
	if err != nil {
		return nil, err
	}

	url, err := r.readString()
	if err != nil {
		return nil, err
	}

	dateModified, err := r.readUint32()
	if err != nil {
		return nil, err
	}

	image, err := r.readString()
	if err != nil {
		return nil, err
	}

	sectionsLen, err := r.readUint32()
	if err != nil {
		return nil, err
	}

	sections := make([]string, sectionsLen)
	for i := range sectionsLen {
		section, err := r.readString()
		if err != nil {
			return nil, err
		}
		sections[i] = section
	}

	abstract, err := r.readString()
	if err != nil {
		return nil, err
	}

	return &article.Article{
		Language:     language,
		Name:         name,
		Url:          url,
		DateModified: dateModified,
		Image:        image,
		Sections:     sections,
		Abstract:     abstract,
	}, nil
}

func (r *Reader) ReadQ1Result() (*result.Q1Result, error) {
	url, err := r.readString()
	if err != nil {
		return nil, err
	}

	dateModified, err := r.readUint32()
	if err != nil {
		return nil, err
	}

	return &result.Q1Result{Url: url, DateModified: dateModified}, nil
}

func (r *Reader) ReadQ2Result() (*result.Q2Result, error) {
	name, err := r.readString()
	if err != nil {
		return nil, err
	}

	url, err := r.readString()
	if err != nil {
		return nil, err
	}

	image, err := r.readBytes()
	if err != nil {
		return nil, err
	}

	return &result.Q2Result{Name: name, Url: url, Image: image}, nil
}

func (r *Reader) ReadQ3Result() (*result.Q3Result, error) {
	word, err := r.readString()
	if err != nil {
		return nil, err
	}

	return &result.Q3Result{Word: word}, nil
}

func (r *Reader) ReadQ4Result() (*result.Q4Result, error) {
	word, err := r.readString()
	if err != nil {
		return nil, err
	}

	count, err := r.readUint32()
	if err != nil {
		return nil, err
	}

	return &result.Q4Result{Word: word, Count: count}, nil
}

func (r *Reader) ReadQ5Result() (*result.Q5Result, error) {
	url, err := r.readString()
	if err != nil {
		return nil, err
	}

	wordCount, err := r.readUint32()
	if err != nil {
		return nil, err
	}

	return &result.Q5Result{Url: url, WordCount: wordCount}, nil
}
