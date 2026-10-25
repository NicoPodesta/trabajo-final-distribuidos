package external

import (
	"fmt"
	"io"

	"trabajo-final-distribuidos/common/messageprotocol/external/safeio"
	"trabajo-final-distribuidos/common/messageprotocol/external/serializer"
)

type MsgType uint32

const (
	ArticleRecord MsgType = iota + 1
	Ack
	EndOfRecords
	Q1Result
	GatewayAssignment
	Q2Result
	Q3Result
	Q4Result
	Q5Result
)

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

func readBytes(reader io.Reader) ([]byte, error) {
	serializedSize, err := safeio.ReadAll(reader, serializer.UINT32_SIZE)
	if err != nil {
		return nil, err
	}
	size := serializer.DeserializeUint32(serializedSize)

	return safeio.ReadAll(reader, size)
}

func WriteAck(writer io.Writer) error {
	return writeMsgType(writer, Ack)
}

func ReadAck(reader io.Reader) error {
	msgType, err := ReadMsgType(reader)
	if err != nil {
		return err
	}
	if msgType != Ack {
		return fmt.Errorf("unexpected message type: %d, expected: %d", msgType, Ack)
	}
	return nil
}

func WriteEndOfRecords(writer io.Writer) error {
	return writeMsgType(writer, EndOfRecords)
}

func ReadEndOfRecords(reader io.Reader) error {
	msgType, err := ReadMsgType(reader)
	if err != nil {
		return err
	}
	if msgType != EndOfRecords {
		return fmt.Errorf("unexpected message type: %d, expected: %d", msgType, EndOfRecords)
	}
	return nil
}

func WriteGatewayAssignment(writer io.Writer, gatewayAddr string) error {
	msg := serializer.SerializeUint32(uint32(GatewayAssignment))
	msg = append(msg, serializer.SerializeString(gatewayAddr)...)
	return safeio.WriteAll(writer, msg)
}

func ReadGatewayAssignment(reader io.Reader) (string, error) {
	msgType, err := ReadMsgType(reader)
	if err != nil {
		return "", err
	}
	if msgType != GatewayAssignment {
		return "", fmt.Errorf("unexpected message type from load balancer: %d", msgType)
	}
	return readString(reader)
}
