package external

import (
	"io"

	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
	"trabajo-final-distribuidos/common/result"
)

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

func WriteQ2Result(writer io.Writer, record *result.Q2Result) error {
	msg := serializer.SerializeUint32(uint32(Q2Result))
	msg = append(msg, serializer.SerializeString(record.Name)...)
	msg = append(msg, serializer.SerializeString(record.Url)...)
	msg = append(msg, serializer.SerializeBytes(record.Image)...)
	return safeio.WriteAll(writer, msg)
}

func ReadQ2Result(reader io.Reader) (*result.Q2Result, error) {
	name, err := readString(reader)
	if err != nil {
		return nil, err
	}
	url, err := readString(reader)
	if err != nil {
		return nil, err
	}
	image, err := readBytes(reader)
	if err != nil {
		return nil, err
	}
	return &result.Q2Result{Name: name, Url: url, Image: image}, nil
}

func WriteQ3Result(writer io.Writer, record *result.Q3Result) error {
	msg := serializer.SerializeUint32(uint32(Q3Result))
	msg = append(msg, serializer.SerializeString(record.Word)...)
	return safeio.WriteAll(writer, msg)
}

func ReadQ3Result(reader io.Reader) (*result.Q3Result, error) {
	word, err := readString(reader)
	if err != nil {
		return nil, err
	}
	return &result.Q3Result{Word: word}, nil
}

func WriteQ4Result(writer io.Writer, record *result.Q4Result) error {
	msg := serializer.SerializeUint32(uint32(Q4Result))
	msg = append(msg, serializer.SerializeString(record.Word)...)
	msg = append(msg, serializer.SerializeUint32(record.Count)...)
	return safeio.WriteAll(writer, msg)
}

func ReadQ4Result(reader io.Reader) (*result.Q4Result, error) {
	word, err := readString(reader)
	if err != nil {
		return nil, err
	}
	serializedCount, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	count := serializer.DeserializeUint32(serializedCount)
	return &result.Q4Result{Word: word, Count: count}, nil
}

func WriteQ5Result(writer io.Writer, record *result.Q5Result) error {
	msg := serializer.SerializeUint32(uint32(Q5Result))
	msg = append(msg, serializer.SerializeString(record.Url)...)
	msg = append(msg, serializer.SerializeUint32(record.WordCount)...)
	return safeio.WriteAll(writer, msg)
}

func ReadQ5Result(reader io.Reader) (*result.Q5Result, error) {
	url, err := readString(reader)
	if err != nil {
		return nil, err
	}
	serializedWordCount, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	wordCount := serializer.DeserializeUint32(serializedWordCount)
	return &result.Q5Result{Url: url, WordCount: wordCount}, nil
}
