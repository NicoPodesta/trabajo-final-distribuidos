package external

import (
	"fmt"

	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
	"trabajo-final-distribuidos/common/result"
)

// Lee el body después de que ReadMsgType consumió Q2ImageChunk
func (r *Reader) ReadQ2ImageChunk() (*result.Q2ImageChunk, error) {
	imageID, err := r.readString()
	if err != nil {
		return nil, err
	}
	seq, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	last, err := r.readUint32()
	if err != nil {
		return nil, err
	}
	if last > 1 {
		return nil, fmt.Errorf("invalid Q2 image chunk last flag: %d", last)
	}

	chunk := &result.Q2ImageChunk{ImageID: imageID, Seq: seq, Last: last == 1}
	if seq == 0 {
		if chunk.Name, err = r.readString(); err != nil {
			return nil, err
		}
		if chunk.Url, err = r.readString(); err != nil {
			return nil, err
		}
	}
	if chunk.Bytes, err = r.readBytes(); err != nil {
		return nil, err
	}
	if err := chunk.Validate(); err != nil {
		return nil, err
	}
	return chunk, nil
}

func (w *Writer) WriteQ2ImageChunk(chunk *result.Q2ImageChunk) error {
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
