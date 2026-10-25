package external

import (
	"io"

	"trabajo-final-distribuidos/common/article"
	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
	"trabajo-final-distribuidos/common/result"
)

type MsgType uint32

const (
	ArticleRecord MsgType = iota + 1
	Ack
	EndOfRecords
	Q1Result
)

func serializeArticleRecord(articleRecord *article.Article) []byte {
	msg := serializer.SerializeString(articleRecord.Language)
	msg = append(msg, serializer.SerializeString(articleRecord.Name)...)
	msg = append(msg, serializer.SerializeString(articleRecord.Url)...)
	msg = append(msg, serializer.SerializeUint32(articleRecord.DateModified)...)
	msg = append(msg, serializer.SerializeString(articleRecord.Image)...)
	msg = append(msg, serializer.SerializeUint32(uint32(len(articleRecord.Sections)))...)
	for _, sec := range articleRecord.Sections {
		msg = append(msg, serializer.SerializeString(sec)...)
	}
	msg = append(msg, serializer.SerializeString(articleRecord.Abstract)...)
	return msg
}

func writeMsgType(writer io.Writer, msgType MsgType) error {
	msg := serializer.SerializeUint32(uint32(msgType))
	return safeio.WriteAll(writer, msg)
}

func ReadMsgType(reader io.Reader) (MsgType, error) {
	msgTypeSerialized, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return 0, err
	}
	msgType := MsgType(serializer.DeserializeUint32(msgTypeSerialized))

	return msgType, nil
}

func readString(reader io.Reader) (string, error) {
	serializedSize, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return "", err
	}
	size := serializer.DeserializeUint32(serializedSize)

	serialized, err := safeio.ReadAll(reader, size)
	if err != nil {
		return "", err
	}
	return serializer.DeserializeString(serialized), nil
}

func WriteArticleRecord(writer io.Writer, articleRecord *article.Article) error {
	msg := serializer.SerializeUint32(uint32(ArticleRecord))
	msg = append(msg, serializeArticleRecord(articleRecord)...)

	return safeio.WriteAll(writer, msg)
}

func ReadArticleRecord(reader io.Reader) (*article.Article, error) {
	language, err := readString(reader)
	if err != nil {
		return nil, err
	}

	name, err := readString(reader)
	if err != nil {
		return nil, err
	}

	url, err := readString(reader)
	if err != nil {
		return nil, err
	}

	serializedDateModified, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	dateModified := serializer.DeserializeUint32(serializedDateModified)

	image, err := readString(reader)
	if err != nil {
		return nil, err
	}

	serializedNumSections, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	numSections := serializer.DeserializeUint32(serializedNumSections)

	sections := make([]string, 0, numSections)
	for i := uint32(0); i < numSections; i++ {
		sec, err := readString(reader)
		if err != nil {
			return nil, err
		}
		sections = append(sections, sec)
	}

	abstract, err := readString(reader)
	if err != nil {
		return nil, err
	}

	articleRecord := article.Article{
		Language:     language,
		Name:         name,
		Url:          url,
		DateModified: dateModified,
		Image:        image,
		Sections:     sections,
		Abstract:     abstract,
	}
	return &articleRecord, nil
}

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func WriteEndOfRecords(writer io.Writer) error {
	return writeMsgType(writer, EndOfRecords)
}

func WriteQ1Result(writer io.Writer, record *result.Q1Result) error {
	msg := serializer.SerializeUint32(uint32(Q1Result))
	msg = append(msg, serializer.SerializeString(record.Url)...)
	msg = append(msg, serializer.SerializeUint32(record.DateModified)...)

	return safeio.WriteAll(writer, msg)
}

func ReadQ1Result(reader io.Reader) (*result.Q1Result, error) {
	url, err := readString(reader)
	if err != nil {
		return nil, err
	}

	serializedDateModified, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	dateModified := serializer.DeserializeUint32(serializedDateModified)

	return &result.Q1Result{Url: url, DateModified: dateModified}, nil
}
