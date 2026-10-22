package safeio

import "io"

func ReadAll(reader io.Reader, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	nread, err := io.ReadFull(reader, buf)
	if err != nil || uint32(nread) < size {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}

func WriteAll(writer io.Writer, msg []byte) error {
	for len(msg) > 0 {
		nwritten, err := writer.Write(msg)
		if err != nil {
			return err
		}
		if nwritten == 0 {
			return io.ErrShortWrite
		}
		msg = msg[nwritten:]
	}
	return nil
}
