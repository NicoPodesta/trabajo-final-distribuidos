package external

import (
	"io"

	"trabajo-final-distribuidos/common/article"
	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
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

	serializedSectionsLen, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	sectionsLen := serializer.DeserializeUint32(serializedSectionsLen)

	sections := make([]string, sectionsLen)
	for i := range sectionsLen {
		section, err := readString(reader)
		if err != nil {
			return nil, err
		}
		sections[i] = section
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
