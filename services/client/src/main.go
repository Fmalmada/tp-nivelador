package main

import (
	"errors"
	"os"
	"strconv"

	client "github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

const DEFAULT_BATCH_SIZE = 1

func loadConfig() (client.ClientConfig, error) {
	agencyId := os.Getenv("AGENCY_ID")
	if agencyId == "" {
		return client.ClientConfig{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_FILE environment variable is required")
	}

	batchSize := DEFAULT_BATCH_SIZE
	if batchSizeEnv := os.Getenv("BATCH_SIZE"); batchSizeEnv != "" {
		parsedBatchSize, err := strconv.Atoi(batchSizeEnv)
		if err != nil {
			return client.ClientConfig{}, errors.New("BATCH_SIZE must be a valid integer")
		}
		batchSize = parsedBatchSize
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  batchSize,
	}, nil
}

func main() {
	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		os.Exit(1)
	}

	c, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		os.Exit(1)
	}

	if err := c.Run(); err != nil {
		os.Exit(1)
	}
}
