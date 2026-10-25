package serializer

import (
	"encoding/binary"
)

const UINT32_SIZE uint32 = 4

func appendLength(data []byte) []byte {
	return append(SerializeUint32(uint32(len(data))), data...)
}

func SerializeString(value string) []byte {
	return SerializeBytes([]byte(value))
}

func SerializeBytes(value []byte) []byte {
	return appendLength(value)
}

func DeserializeString(bytes []byte) string {
	return string(bytes)
}

func SerializeUint32(value uint32) []byte {
	data := make([]byte, UINT32_SIZE)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func DeserializeUint32(bytes []byte) uint32 {
	return binary.BigEndian.Uint32(bytes)
}
