package external

import (
	"io"
	"sync"

	"trabajo-final-distribuidos/common/article"
	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
	"trabajo-final-distribuidos/common/result"
)

type Writer struct {
	w         io.Writer
	writeLock sync.Mutex
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) writeMsgType(msgType MsgType) error {
	msg := serializer.SerializeUint32(uint32(msgType))
	return safeio.WriteAll(w.w, msg)
}

func (w *Writer) WriteAck() error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	return w.writeMsgType(Ack)
}

func (w *Writer) WriteEndOfRecords() error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	return w.writeMsgType(EndOfRecords)
}

func (w *Writer) WriteGatewayAssignment(gatewayAddr string) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	msg := serializer.SerializeUint32(uint32(GatewayAssignment))
	msg = append(msg, serializer.SerializeString(gatewayAddr)...)
	return safeio.WriteAll(w.w, msg)
}

func serializeArticleFields(articleRecord *article.Article) []byte {
	var msg []byte
	msg = append(msg, serializer.SerializeString(articleRecord.Language)...)
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

func (w *Writer) WriteArticleBatch(records []article.Article) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	msg := serializer.SerializeUint32(uint32(ArticleBatch))
	msg = append(msg, serializer.SerializeUint32(uint32(len(records)))...)
	for i := range records {
		msg = append(msg, serializeArticleFields(&records[i])...)
	}
	return safeio.WriteAll(w.w, msg)
}

func (w *Writer) WriteQ1Result(record *result.Q1Result) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	msg := serializer.SerializeUint32(uint32(Q1Result))
	msg = append(msg, serializer.SerializeString(record.Url)...)
	msg = append(msg, serializer.SerializeUint32(record.DateModified)...)
	return safeio.WriteAll(w.w, msg)
}

func (w *Writer) WriteQ2Result(record *result.Q2Result) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	msg := serializer.SerializeUint32(uint32(Q2Result))
	msg = append(msg, serializer.SerializeString(record.Name)...)
	msg = append(msg, serializer.SerializeString(record.Url)...)
	msg = append(msg, serializer.SerializeBytes(record.Image)...)
	return safeio.WriteAll(w.w, msg)
}

func (w *Writer) WriteQ2ImageChunk(chunk *result.Q2ImageChunk) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	if err := chunk.Validate(); err != nil {
		return err
	}
	var last uint32
	if chunk.Last {
		last = 1
	}

	msg := serializer.SerializeUint32(uint32(Q2ImageChunk))
	msg = append(msg, serializer.SerializeString(chunk.ImageID)...)
	msg = append(msg, serializer.SerializeUint32(chunk.Seq)...)
	msg = append(msg, serializer.SerializeUint32(last)...)
	if chunk.Seq == 0 {
		msg = append(msg, serializer.SerializeString(chunk.Name)...)
		msg = append(msg, serializer.SerializeString(chunk.Url)...)
	}
	msg = append(msg, serializer.SerializeBytes(chunk.Bytes)...)
	return safeio.WriteAll(w.w, msg)
}

func (w *Writer) WriteQ3Result(record *result.Q3Result) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	msg := serializer.SerializeUint32(uint32(Q3Result))
	msg = append(msg, serializer.SerializeString(record.Word)...)
	return safeio.WriteAll(w.w, msg)
}

func (w *Writer) WriteQ4Result(record *result.Q4Result) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	msg := serializer.SerializeUint32(uint32(Q4Result))
	msg = append(msg, serializer.SerializeString(record.Word)...)
	msg = append(msg, serializer.SerializeUint32(record.Count)...)
	return safeio.WriteAll(w.w, msg)
}

func (w *Writer) WriteQ5Result(record *result.Q5Result) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	msg := serializer.SerializeUint32(uint32(Q5Result))
	msg = append(msg, serializer.SerializeString(record.Url)...)
	msg = append(msg, serializer.SerializeUint32(record.WordCount)...)
	return safeio.WriteAll(w.w, msg)
}
