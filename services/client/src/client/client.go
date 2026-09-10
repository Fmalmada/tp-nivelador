package client

import (
	"bufio"
	"errors"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 15
const CONNECTION_ATTEMPS_DELAY_MS = 500
const GC_RELEASE_INTERVAL_BATCHES = 50

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn         net.Conn
	config       ClientConfig
	shuttingDown atomic.Bool
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}
	return &Client{conn: conn, config: config}, nil
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

func (client *Client) watchSigterm() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGTERM)
	go func() {
		<-signalChannel
		logger.Info("sigterm", logger.InProgress)
		client.shuttingDown.Store(true)
		client.conn.Close()
	}()
}

func (client *Client) handleRunError(action string, err error) error {
	if client.shuttingDown.Load() {
		logger.Info(action, logger.Success, "reason", "sigterm")
		return nil
	}
	logger.Error(action, logger.Fail, "err", err)
	return err
}

func (client *Client) sendHello() error {
	agencyId, err := strconv.Atoi(client.config.AgencyId)
	if err != nil {
		return err
	}
	payload := []byte(strconv.Itoa(agencyId))
	if err := protocol.SendMessage(client.conn, protocol.Hello, payload); err != nil {
		return client.handleRunError("send-hello", err)
	}
	return nil
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

func (client *Client) sendDone() error {
	if err := protocol.SendMessage(client.conn, protocol.Done, nil); err != nil {
		return client.handleRunError("send-done", err)
	}
	return nil
}

func (client *Client) receiveWinners() ([]string, error) {
	messageType, payload, err := protocol.RecvMessage(client.conn)
	if err != nil {
		return nil, client.handleRunError("receive-winners", err)
	}
	if messageType != protocol.Winners {
		return nil, errors.New("expected WINNERS from server")
	}
	return protocol.DecodeWinners(payload), nil
}

func (client *Client) sendBetsFromInputFile() error {
	inputFile, err := os.Open(client.config.InputFile)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	sender := newBatchSender(client, client.config.BatchSize)

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if err := sender.add(line); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return sender.flush()
}

func writeWinnersToOutputFile(outputPath string, winners []string) error {
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	for _, winner := range winners {
		if _, err := writer.WriteString(winner + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) Run() error {
	const mainAction = "run-agency"
	defer client.conn.Close()
	client.watchSigterm()

	logger.Info(mainAction, logger.InProgress, "agency-id", client.config.AgencyId)

	if err := client.sendHello(); err != nil {
		return err
	}
	if err := client.sendBetsFromInputFile(); err != nil {
		return err
	}
	if err := client.sendDone(); err != nil {
		return err
	}

	winners, err := client.receiveWinners()
	if err != nil {
		return err
	}
	if err := writeWinnersToOutputFile(client.config.OutputFile, winners); err != nil {
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)
	return nil
}
