package common

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")


const (
	maxBatchSizeBytes    = 8192
	maxBetSizeBytes      = 512
	safetyMargin         = maxBetSizeBytes
	maxSafeBatchSize     = maxBatchSizeBytes - safetyMargin
	batchHeaderSize      = 4 + 1 + 2 // length(4) + agenciaID(1) + bet_count(2)
	betFixedFieldsSize   = 1 + 1 + 4 + 4 + 2 // nombreLen(1) + apellidoLen(1) + documento(4) + nacimiento(4) + numero(2)
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchMaxAmount int
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
	shutdownChan chan bool
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
		shutdownChan: make(chan bool, 1),
	}

	client.setupSignalHandler()
	
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	defer c.cleanup()
	
	cliIDStr := os.Getenv("CLI_ID")
	if cliIDStr == "" {
		log.Errorf("action: read_agency_id | result: fail | client_id: %v | error: CLI_ID environment variable is required", 
			c.config.ID)
		return
	}
	agenciaID, err := strconv.ParseUint(cliIDStr, 10, 8)
	if err != nil {
		log.Errorf("action: parse_agency_id | result: fail | client_id: %v | error: %v", 
			c.config.ID, err)
		return
	}
	
	filename := fmt.Sprintf("/data/agency-%s.csv", c.config.ID)
	
	if err := c.createClientSocket(); err != nil {
		return
	}
	defer c.conn.Close()

	batchID := 1
	err = ProcessCSVStreaming(filename, uint8(agenciaID), c.config.BatchMaxAmount, func(batch *Batch, isLast bool) error {
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_requested | result: success | client_id: %v", c.config.ID)
			return fmt.Errorf("shutdown requested")
		default:
		}

		if batchID > c.config.LoopAmount {
			return fmt.Errorf("batch limit reached")
		}

		if isLast {
			batch.IsLastBatch = true
		}

		if err := c.sendBatch(batch); err != nil {
			log.Errorf("action: enviar_batch | result: fail | client_id: %v | batch_id: %d | error: %v", 
				c.config.ID, batchID, err)
			batchID++
			return nil 
		}
		
		response, err := c.receiveResponse()
		if err != nil {
			log.Errorf("action: receive_response | result: fail | client_id: %v | batch_id: %d | error: %v",
				c.config.ID, batchID, err)
			batchID++
			return nil
		}
		
		if response.Status == STATUS_OK {
			log.Infof("action: batch_enviado | result: success | client_id: %v | batch_id: %d | cantidad: %d",
				c.config.ID, batchID, len(batch.Apuestas))
		} else {
			log.Errorf("action: batch_enviado | result: fail | client_id: %v | batch_id: %d | cantidad: %d | error: %s",
				c.config.ID, batchID, len(batch.Apuestas), response.Message)
		}
		
		if batch.IsLastBatch {
			c.queryWinners(uint8(agenciaID))
		}
		
		batchID++
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_requested | result: success | client_id: %v", c.config.ID)
			return fmt.Errorf("shutdown requested")
		case <-time.After(c.config.LoopPeriod):
		}
		
		return nil
	})
	
	if err != nil {
		log.Errorf("action: process_csv_streaming | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	
	log.Infof("action: loop_finished | result: success | client_id: %v | batches_sent: %d", c.config.ID, batchID-1)
}

// setupSignalHandler configures signal handling for graceful shutdown
func (c *Client) setupSignalHandler() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)
	
	go func() {
		sig := <-signalChan
		log.Infof("action: signal_received | result: success | client_id: %v | signal: %v", c.config.ID, sig)
		
		select {
		case c.shutdownChan <- true:
		default:
		}
	}()
}

// sendBet sends a bet to the server
func (c *Client) sendBet(bet *Bet) error {
	return SendBet(c.conn, bet)
}

// receiveResponse receives and parses the server response
func (c *Client) receiveResponse() (*BetResponse, error) {
	return ReceiveResponse(c.conn)
}

// queryWinners queries the server for winners of this agency with retries
func (c *Client) queryWinners(agenciaID uint8) {
	maxRetries := 10
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := SendQueryWinners(c.conn, agenciaID); err != nil {
			log.Errorf("action: query_winners | result: fail | client_id: %v | attempt: %d | error: %v", c.config.ID, attempt, err)
			if attempt < maxRetries {
				time.Sleep(1 * time.Second)
			}
			continue
		}
		
		lengthData, err := recvAll(c.conn, 4)
		if err != nil {
			log.Errorf("action: query_winners | result: fail | client_id: %v | attempt: %d | error: failed to read response length: %v", c.config.ID, attempt, err)
			if attempt < maxRetries {
				time.Sleep(1 * time.Second)
			}
			continue
		}
		
		length := binary.BigEndian.Uint32(lengthData)
		responseData, err := recvAll(c.conn, int(length))
		if err != nil {
			log.Errorf("action: query_winners | result: fail | client_id: %v | attempt: %d | error: failed to read response: %v", c.config.ID, attempt, err)
			if attempt < maxRetries {
				time.Sleep(1 * time.Second)
			}
			continue
		}
		
		fullData := append(lengthData, responseData...)
		winnersResp, err := DeserializeWinnersResponse(fullData)
		if err != nil {
			log.Errorf("action: query_winners | result: fail | client_id: %v | attempt: %d | error: failed to deserialize response: %v", c.config.ID, attempt, err)
			if attempt < maxRetries {
				time.Sleep(1 * time.Second)
			}
			continue
		}
		
		if winnersResp.Status != STATUS_OK {
			log.Errorf("action: query_winners | result: fail | client_id: %v | attempt: %d | error: server returned error", c.config.ID, attempt)
			if attempt < maxRetries {
				time.Sleep(1 * time.Second)
			}
			continue
		}
		
		if len(winnersResp.WinnerDNIs) == 0 && attempt < maxRetries {
			log.Debugf("action: query_winners | result: retry | client_id: %v | attempt: %d | reason: zero_winners_retry", c.config.ID, attempt)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", len(winnersResp.WinnerDNIs))
		return
	}
	
	log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | reason: max_retries_exceeded", c.config.ID)
}



// sendBatch sends a batch of bets to the server
func (c *Client) sendBatch(batch *Batch) error {
	return SendBatch(c.conn, batch)
}

// createBatches splits bets into batches respecting maxAmount and 8KB size limit
func (c *Client) createBatches(bets []Bet, agenciaID uint8) []Batch {
	var batches []Batch
	maxAmount := c.config.BatchMaxAmount
	
	currentBatch := Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0), IsLastBatch: false}
	currentSizeBytes := batchHeaderSize
	
	for _, bet := range bets {
		if len(currentBatch.Apuestas) >= maxAmount {
			if len(currentBatch.Apuestas) > 0 {
				batches = append(batches, currentBatch)
			}
			currentBatch = Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0), IsLastBatch: false}
			currentSizeBytes = batchHeaderSize
		}
		
		estimatedBetSize := betFixedFieldsSize + len(bet.Nombre) + len(bet.Apellido)
		if currentSizeBytes + estimatedBetSize > maxSafeBatchSize {
			if len(currentBatch.Apuestas) > 0 {
				batches = append(batches, currentBatch)
			}
			currentBatch = Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0), IsLastBatch: false}
			currentSizeBytes = batchHeaderSize
		}
		
		currentBatch.Apuestas = append(currentBatch.Apuestas, bet)
		currentSizeBytes += estimatedBetSize
	}
	
	if len(currentBatch.Apuestas) > 0 {
		batches = append(batches, currentBatch)
	}
	
	if len(batches) > 0 {
		batches[len(batches)-1].IsLastBatch = true
	}
	
	return batches
}

// cleanup closes connection and logs shutdown
func (c *Client) cleanup() {
	log.Infof("action: client_shutdown | result: in_progress | client_id: %v", c.config.ID)
	
	if c.conn != nil {
		c.conn.Close()
		log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
	}
	
	log.Infof("action: client_shutdown | result: success | client_id: %v", c.config.ID)
}
