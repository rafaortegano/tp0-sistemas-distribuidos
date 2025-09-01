package common

import (
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
	bets, skippedCount, err := ReadBetsFromCSV(filename, uint8(agenciaID))
	if err != nil {
		log.Errorf("action: read_csv | result: fail | client_id: %v | file: %s | error: %v", 
			c.config.ID, filename, err)
		return
	}
	
	if skippedCount > 0 {
		log.Debugf("action: read_csv | result: partial | client_id: %v | valid_bets: %d | skipped_bets: %d", 
			c.config.ID, len(bets), skippedCount)
	}
	
	if len(bets) == 0 {
		log.Infof("action: read_csv | result: success | client_id: %v | total_bets: 0", c.config.ID)
		return
	}
	
	batches := c.createBatches(bets, uint8(agenciaID))
	
	
	// There is an autoincremental batchID to identify every batch sent  
	// Send batches if the batch amount threshold has not been surpassed
	batchesSent := 0
	for batchID := 1; batchID <= c.config.LoopAmount && batchesSent < len(batches); batchID++ {
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_requested | result: success | client_id: %v", c.config.ID)
			return
		default:
		}
		
		batch := batches[batchesSent]
		
		
		if err := c.createClientSocket(); err != nil {
			return
		}

		if err := c.sendBatch(&batch); err != nil {
			log.Errorf("action: enviar_batch | result: fail | client_id: %v | batch_id: %d | error: %v", 
				c.config.ID, batchID, err)
			c.conn.Close()
			return
		}
		
		response, err := c.receiveResponse()
		if err != nil {
			log.Errorf("action: receive_response | result: fail | client_id: %v | batch_id: %d | error: %v",
				c.config.ID, batchID, err)
			c.conn.Close()
			return
		}
		
		c.conn.Close()
		
		if response.Status == STATUS_OK {
			log.Infof("action: batch_enviado | result: success | client_id: %v | batch_id: %d | cantidad: %d",
				c.config.ID, batchID, len(batch.Apuestas))
		} else {
			log.Errorf("action: batch_enviado | result: fail | client_id: %v | batch_id: %d | cantidad: %d | error: %s",
				c.config.ID, batchID, len(batch.Apuestas), response.Message)
		}
		
		batchesSent++
		
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_requested | result: success | client_id: %v", c.config.ID)
			return
		case <-time.After(c.config.LoopPeriod):
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v | batches_sent: %d", c.config.ID, batchesSent)
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

// sendBatch sends a batch of bets to the server
func (c *Client) sendBatch(batch *Batch) error {
	return SendBatch(c.conn, batch)
}

// createBatches splits bets into batches respecting maxAmount and 8KB size limit
func (c *Client) createBatches(bets []Bet, agenciaID uint8) []Batch {
	var batches []Batch
	maxAmount := c.config.BatchMaxAmount
	
	currentBatch := Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0)}
	currentSizeBytes := batchHeaderSize
	
	for _, bet := range bets {
		if len(currentBatch.Apuestas) >= maxAmount {
			if len(currentBatch.Apuestas) > 0 {
				batches = append(batches, currentBatch)
			}
			currentBatch = Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0)}
			currentSizeBytes = batchHeaderSize
		}
		
		estimatedBetSize := betFixedFieldsSize + len(bet.Nombre) + len(bet.Apellido)
		if currentSizeBytes + estimatedBetSize > maxSafeBatchSize {
			if len(currentBatch.Apuestas) > 0 {
				batches = append(batches, currentBatch)
			}
			currentBatch = Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0)}
			currentSizeBytes = batchHeaderSize
		}
		
		currentBatch.Apuestas = append(currentBatch.Apuestas, bet)
		currentSizeBytes += estimatedBetSize
	}
	
	if len(currentBatch.Apuestas) > 0 {
		batches = append(batches, currentBatch)
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
