package common

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
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
	
	bet, err := NewBetFromEnv()
	if err != nil {
		log.Errorf("action: read_bet_env | result: fail | client_id: %v | error: %v", 
			c.config.ID, err)
		return
	}

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_requested | result: success | client_id: %v", c.config.ID)
			return
		default:
		}
		
		if err := c.createClientSocket(); err != nil {
			return
		}

		if err := c.sendBet(bet); err != nil {
			log.Errorf("action: enviar_apuesta | result: fail | client_id: %v | error: %v", 
				c.config.ID, err)
			c.conn.Close()
			return
		}
		
		response, err := c.receiveResponse()
		if err != nil {
			log.Errorf("action: receive_response | result: fail | client_id: %v | error: %v",
				c.config.ID, err)
			c.conn.Close()
			return
		}
		
		c.conn.Close()
		
		if response.Status == STATUS_OK {
			log.Infof("action: apuesta_enviada | result: success | dni: %d | numero: %d",
				bet.Documento, bet.Numero)
		} else {
			log.Errorf("action: apuesta_enviada | result: fail | dni: %d | numero: %d | error: %s",
				bet.Documento, bet.Numero, response.Message)
		}
		
		select {
		case <-c.shutdownChan:
			log.Infof("action: shutdown_requested | result: success | client_id: %v", c.config.ID)
			return
		case <-time.After(c.config.LoopPeriod):
		}
	}
	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
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

// cleanup closes connection and logs shutdown
func (c *Client) cleanup() {
	log.Infof("action: client_shutdown | result: in_progress | client_id: %v", c.config.ID)
	
	if c.conn != nil {
		c.conn.Close()
		log.Infof("action: close_connection | result: success | client_id: %v", c.config.ID)
	}
	
	log.Infof("action: client_shutdown | result: success | client_id: %v", c.config.ID)
}
