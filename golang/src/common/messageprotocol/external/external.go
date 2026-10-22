package external

import (
	"io"

	"trabajo-final-distribuidos/common/article"
	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
)

type MsgType uint32

const (
	ArticleRecord MsgType = iota + 1
	Ack
	EndOfRecords
)

func serializeArticleRecord(articleRecord *article.Article) []byte {
	msg := serializer.SerializeString(articleRecord.Language)
	msg = append(msg, serializer.SerializeString(articleRecord.Url)...)
	msg = append(msg, serializer.SerializeUint32(articleRecord.DateModified)...)
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

	url, err := readString(reader)
	if err != nil {
		return nil, err
	}

	serializedDateModified, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	dateModified := serializer.DeserializeUint32(serializedDateModified)

	articleRecord := article.Article{Language: language, Url: url, DateModified: dateModified}
	return &articleRecord, nil
}

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func WriteEndOfRecords(writer io.Writer) error {
	return writeMsgType(writer, EndOfRecords)
}
