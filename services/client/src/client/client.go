package client

import (
	"bufio"
	"errors"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 15
const CONNECTION_ATTEMPS_DELAY_MS = 500

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) sendBatch(records []string) error {
	payload := protocol.EncodeBetBatch(records)
	if err := protocol.SendMessage(client.conn, protocol.BetBatch, payload); err != nil {
		return err
	}

	messageType, ackPayload, err := protocol.RecvMessage(client.conn)
	if err != nil {
		return err
	}
	if messageType != protocol.Ack {
		return errors.New("expected ACK from server")
	}

	stored, err := protocol.DecodeAck(ackPayload)
	if err != nil {
		return err
	}
	if stored != len(records) {
		return errors.New("server stored fewer bets than sent")
	}
	return nil
}

func (client *Client) Run() error {
	const mainAction = "run-agency"
	defer client.conn.Close()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	agencyId, err := strconv.Atoi(client.config.AgencyId)
	if err != nil {
		return err
	}
	if err := protocol.SendMessage(client.conn, protocol.Hello, []byte(strconv.Itoa(agencyId))); err != nil {
		return err
	}

	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	batch := make([]string, 0, client.config.BatchSize)
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		batch = append(batch, line)
		if len(batch) == client.config.BatchSize {
			if err := client.sendBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := client.sendBatch(batch); err != nil {
			return err
		}
	}

	if err := protocol.SendMessage(client.conn, protocol.Done, nil); err != nil {
		return err
	}

	messageType, payload, err := protocol.RecvMessage(client.conn)
	if err != nil {
		return err
	}
	if messageType != protocol.Winners {
		return errors.New("expected WINNERS from server")
	}

	outputFile, err := os.Create(client.config.OutputFile)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()
	for _, winner := range protocol.DecodeWinners(payload) {
		if _, err := writer.WriteString(winner + "\n"); err != nil {
			return err
		}
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	return nil
}
