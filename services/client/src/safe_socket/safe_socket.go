package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(writer io.Writer, data []byte) error {
	totalWritten := 0
	for totalWritten < len(data) {
		n, err := writer.Write(data[totalWritten:])
		totalWritten += n
		if err != nil {
			return err
		}
	}
	return nil
}

func RecvAll(reader io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	totalRead := 0
	for totalRead < size {
		n, err := reader.Read(buff[totalRead:])
		totalRead += n
		if err != nil {
			if err == io.EOF && totalRead == size {
				break
			}
			return nil, err
		}
	}
	return buff, nil
}
