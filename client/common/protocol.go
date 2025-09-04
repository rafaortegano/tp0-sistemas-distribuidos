package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	STATUS_OK    = 0x00
	STATUS_ERROR = 0x01
	
	MSG_TYPE_BATCH         = 0x01
	MSG_TYPE_QUERY_WINNERS = 0x02
)

type BetResponse struct {
	Status  uint8
	Message string
}

// QueryWinnersRequest represents a request to query winners for an agency
type QueryWinnersRequest struct {
	AgenciaID uint8
}

// WinnersResponse represents the response with winners for an agency
type WinnersResponse struct {
	Status       uint8
	WinnerCount  uint16
	WinnerDNIs   []uint32
}

// SerializeBatch serializes a batch of bets to binary
func SerializeBatch(batch *Batch) ([]byte, error) {
	payloadBuf := new(bytes.Buffer)
	

	binary.Write(payloadBuf, binary.BigEndian, uint8(MSG_TYPE_BATCH))
	
	binary.Write(payloadBuf, binary.BigEndian, batch.AgenciaID)
	
	var isLastByte uint8 = 0
	if batch.IsLastBatch {
		isLastByte = 1
	}
	binary.Write(payloadBuf, binary.BigEndian, isLastByte)
	
	binary.Write(payloadBuf, binary.BigEndian, uint16(len(batch.Apuestas)))

	for _, bet := range batch.Apuestas {
		if err := bet.Validate(); err != nil {
			return nil, fmt.Errorf("invalid bet in batch: %v", err)
		}
		
		if err := writeString(payloadBuf, bet.Nombre); err != nil {
			return nil, err
		}
		 
		if err := writeString(payloadBuf, bet.Apellido); err != nil {
			return nil, err
		}
		
		binary.Write(payloadBuf, binary.BigEndian, bet.Documento)
		
		binary.Write(payloadBuf, binary.BigEndian, uint16(bet.Nacimiento/10000)) 
		binary.Write(payloadBuf, binary.BigEndian, uint8((bet.Nacimiento/100)%100)) 
		binary.Write(payloadBuf, binary.BigEndian, uint8(bet.Nacimiento%100)) 
		
		binary.Write(payloadBuf, binary.BigEndian, uint16(bet.Numero))
	}
	
	payload := payloadBuf.Bytes()
	finalBuf := new(bytes.Buffer)
	binary.Write(finalBuf, binary.BigEndian, uint32(len(payload)))
	finalBuf.Write(payload)
	
	return finalBuf.Bytes(), nil
}

// SerializeBet serializes a bet to binary format with length prefix
func SerializeBet(bet *Bet) ([]byte, error) {
	if err := bet.Validate(); err != nil {
		return nil, err
	}
	
	payloadBuf := new(bytes.Buffer)
	
	if err := writeString(payloadBuf, bet.Nombre); err != nil {
		return nil, err
	}
	 
	if err := writeString(payloadBuf, bet.Apellido); err != nil {
		return nil, err
	}
	
	binary.Write(payloadBuf, binary.BigEndian, bet.Documento)
	
	binary.Write(payloadBuf, binary.BigEndian, uint16(bet.Nacimiento/10000)) 
	binary.Write(payloadBuf, binary.BigEndian, uint8((bet.Nacimiento/100)%100)) 
	binary.Write(payloadBuf, binary.BigEndian, uint8(bet.Nacimiento%100)) 
	
	binary.Write(payloadBuf, binary.BigEndian, uint16(bet.Numero))
	
	payload := payloadBuf.Bytes()
	finalBuf := new(bytes.Buffer)
	binary.Write(finalBuf, binary.BigEndian, uint32(len(payload)))
	finalBuf.Write(payload)
	
	return finalBuf.Bytes(), nil
}

func DeserializeBetResponse(data []byte) (*BetResponse, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("response too short")
	}
	
	response := &BetResponse{
		Status: data[0],
	}
	
	if len(data) > 1 {
		messageLen := int(data[1])
		if len(data) < 2+messageLen {
			return nil, fmt.Errorf("incomplete message")
		}
		response.Message = string(data[2 : 2+messageLen])
	}
	
	return response, nil
}

func writeString(buf *bytes.Buffer, s string) error {
	if len(s) > 255 {
		return fmt.Errorf("string too long: %d bytes (max 255)", len(s))
	}
	buf.WriteByte(byte(len(s)))
	buf.WriteString(s)
	return nil
}

// ReceiveResponse receives and deserializes a bet response from connection
func ReceiveResponse(reader io.Reader) (*BetResponse, error) {
	lengthData, err := recvAll(reader, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}
	
	length := binary.BigEndian.Uint32(lengthData)
	
	responseData, err := recvAll(reader, int(length))
	if err != nil {
		return nil, fmt.Errorf("failed to read response payload: %v", err)
	}
	
	return DeserializeBetResponse(responseData)
}

// recvAll reads exactly 'length' bytes from connection
func recvAll(reader io.Reader, length int) ([]byte, error) {
	data := make([]byte, length)
	totalRead := 0
	
	for totalRead < length {
		n, err := reader.Read(data[totalRead:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, fmt.Errorf("connection closed")
		}
		totalRead += n
	}
	
	return data, nil
}

// SendBet serializes and sends a bet to connection
func SendBet(writer io.Writer, bet *Bet) error {
	data, err := SerializeBet(bet)
	if err != nil {
		return err
	}
	return sendAll(writer, data)
}

// sendAll writes all bytes to connection
func sendAll(writer io.Writer, data []byte) error {
	totalWritten := 0
	
	for totalWritten < len(data) {
		n, err := writer.Write(data[totalWritten:])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("connection closed")
		}
		totalWritten += n
	}
	
	return nil
}

// SendBatch sends a batch of bets to the server
func SendBatch(writer io.Writer, batch *Batch) error {
	data, err := SerializeBatch(batch)
	if err != nil {
		return fmt.Errorf("failed to serialize batch: %v", err)
	}
	
	return sendAll(writer, data)
}

// SerializeQueryWinners serializes a query winners request to binary
func SerializeQueryWinners(request *QueryWinnersRequest) ([]byte, error) {
	payloadBuf := new(bytes.Buffer)
	
	binary.Write(payloadBuf, binary.BigEndian, uint8(MSG_TYPE_QUERY_WINNERS))
	
	binary.Write(payloadBuf, binary.BigEndian, request.AgenciaID)
	
	payload := payloadBuf.Bytes()
	finalBuf := new(bytes.Buffer)
	binary.Write(finalBuf, binary.BigEndian, uint32(len(payload)))
	finalBuf.Write(payload)
	
	return finalBuf.Bytes(), nil
}

// DeserializeWinnersResponse deserializes a winners response from binary
func DeserializeWinnersResponse(data []byte) (*WinnersResponse, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for response length")
	}
	
	buf := bytes.NewReader(data)
	
	var length uint32
	if err := binary.Read(buf, binary.BigEndian, &length); err != nil {
		return nil, fmt.Errorf("failed to read response length: %v", err)
	}
	
	if len(data) != int(4+length) {
		return nil, fmt.Errorf("response length mismatch")
	}
	
	var status uint8
	if err := binary.Read(buf, binary.BigEndian, &status); err != nil {
		return nil, fmt.Errorf("failed to read status: %v", err)
	}
	
	var winnerCount uint16
	if err := binary.Read(buf, binary.BigEndian, &winnerCount); err != nil {
		return nil, fmt.Errorf("failed to read winner count: %v", err)
	}
	
	winnerDNIs := make([]uint32, winnerCount)
	for i := uint16(0); i < winnerCount; i++ {
		var dni uint32
		if err := binary.Read(buf, binary.BigEndian, &dni); err != nil {
			return nil, fmt.Errorf("failed to read DNI %d: %v", i, err)
		}
		winnerDNIs[i] = dni
	}
	
	return &WinnersResponse{
		Status:      status,
		WinnerCount: winnerCount,
		WinnerDNIs:  winnerDNIs,
	}, nil
}

// SendQueryWinners sends a query winners request
func SendQueryWinners(writer io.Writer, agenciaID uint8) error {
	request := &QueryWinnersRequest{AgenciaID: agenciaID}
	data, err := SerializeQueryWinners(request)
	if err != nil {
		return fmt.Errorf("failed to serialize query winners request: %v", err)
	}
	
	return sendAll(writer, data)
}

