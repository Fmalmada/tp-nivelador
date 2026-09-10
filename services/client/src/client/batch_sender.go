package client

import "runtime/debug"

type batchSender struct {
	client      *Client
	batchSize   int
	buffer      []string
	batchesSent int
}

func newBatchSender(client *Client, batchSize int) *batchSender {
	return &batchSender{
		client:    client,
		batchSize: batchSize,
		buffer:    make([]string, 0, batchSize),
	}
}

func (s *batchSender) add(line string) error {
	s.buffer = append(s.buffer, line)
	if len(s.buffer) < s.batchSize {
		return nil
	}
	return s.sendCurrentBatch()
}

func (s *batchSender) flush() error {
	if len(s.buffer) == 0 {
		return nil
	}
	return s.sendCurrentBatch()
}

func (s *batchSender) sendCurrentBatch() error {
	if err := s.client.sendBatch(s.buffer); err != nil {
		return s.client.handleRunError("send-batch", err)
	}
	s.buffer = s.buffer[:0]

	s.batchesSent++
	if s.batchesSent%GC_RELEASE_INTERVAL_BATCHES == 0 {
		debug.FreeOSMemory()
	}
	return nil
}
