package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

type MessageType byte

const (
	Hello    MessageType = 'H'
	BetBatch MessageType = 'B'
	Done     MessageType = 'D'
	Ack      MessageType = 'A'
	Winners  MessageType = 'W'
)

const _headerSize = 4
const _recordSep = "\n"

func SendMessage(writer io.Writer, messageType MessageType, payload []byte) error {
	body := make([]byte, 0, len(payload)+1)
	body = append(body, byte(messageType))
	body = append(body, payload...)

	header := make([]byte, _headerSize)
	binary.BigEndian.PutUint32(header, uint32(len(body)))

	fullMessage := make([]byte, 0, len(header)+len(body))
	fullMessage = append(fullMessage, header...)
	fullMessage = append(fullMessage, body...)

	return safe_socket.SendAll(writer, fullMessage)
}

func RecvMessage(reader io.Reader) (MessageType, []byte, error) {
	header, err := safe_socket.RecvAll(reader, _headerSize)
	if err != nil {
		return 0, nil, err
	}
	bodyLength := binary.BigEndian.Uint32(header)
	if bodyLength == 0 {
		return 0, nil, errors.New("received a message with an empty body")
	}
	body, err := safe_socket.RecvAll(reader, int(bodyLength))
	if err != nil {
		return 0, nil, err
	}
	return MessageType(body[0]), body[1:], nil
}

func EncodeBetBatch(records []string) []byte {
	return []byte(strings.Join(records, _recordSep))
}

func DecodeAck(payload []byte) (int, error) {
	return strconv.Atoi(string(payload))
}

func DecodeWinners(payload []byte) []string {
	if len(payload) == 0 {
		return nil
	}
	return strings.Split(string(payload), _recordSep)
}
