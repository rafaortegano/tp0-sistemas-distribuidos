package common

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// reads bets from a CSV file and returns only valid ones
// filename should be the full path to the CSV file
func ReadBetsFromCSV(filename string, agenciaID uint8) ([]Bet, int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open CSV file %s: %v", filename, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read CSV file %s: %v", filename, err)
	}

	validBets := []Bet{}
	skippedCount := 0

	for lineNum := 0; lineNum < len(records); lineNum++ {
		record := records[lineNum]
		bet, err := parseBetFromCSV(record, agenciaID, lineNum+1)
		if err != nil {
			log.Debugf("action: bet_skipped | line: %d | error: %v", lineNum+1, err)
			skippedCount++
			continue
		}
		validBets = append(validBets, *bet)
	}

	return validBets, skippedCount, nil
}

// parseBetFromCSV parses a single CSV record into a Bet
func parseBetFromCSV(record []string, agenciaID uint8, lineNum int) (*Bet, error) {
	if len(record) != 5 {
		return nil, fmt.Errorf("invalid number of fields: expected 5, got %d", len(record))
	}

	bet := &Bet{}
	bet.Nombre = record[0]
	bet.Apellido = record[1]

	documento, err := strconv.ParseUint(record[2], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid documento: %s", record[2])
	}
	bet.Documento = uint32(documento)

	birthdate := strings.ReplaceAll(record[3], "-", "")
	nacimiento, err := strconv.ParseUint(birthdate, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid birthdate: %s", record[3])
	}
	bet.Nacimiento = uint32(nacimiento)

	numero, err := strconv.ParseUint(record[4], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid numero: %s", record[4])
	}
	bet.Numero = uint32(numero)

	if err := bet.Validate(); err != nil {
		return nil, fmt.Errorf("validation failed: %v", err)
	}

	return bet, nil
}

// ProcessCSVStreaming reads CSV file line by line and calls batchHandler for each completed batch
func ProcessCSVStreaming(filename string, agenciaID uint8, maxBatchSize int, batchHandler func(*Batch, bool) error) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %v", filename, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	currentBatch := Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0, maxBatchSize)}
	currentSizeBytes := batchHeaderSize
	skippedCount := 0
	validBetsCount := 0
	lineNum := 0

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read CSV line %d: %v", lineNum+1, err)
		}
		
		lineNum++
		bet, err := parseBetFromCSV(record, agenciaID, lineNum)
		if err != nil {
			log.Debugf("action: bet_skipped | line: %d | error: %v", lineNum, err)
			skippedCount++
			continue
		}

		estimatedBetSize := betFixedFieldsSize + len(bet.Nombre) + len(bet.Apellido)
		if len(currentBatch.Apuestas) >= maxBatchSize || 
		   (len(currentBatch.Apuestas) > 0 && currentSizeBytes + estimatedBetSize > maxSafeBatchSize) {
			
			if err := batchHandler(&currentBatch, false); err != nil {
				return err
			}
			
			currentBatch = Batch{AgenciaID: agenciaID, Apuestas: make([]Bet, 0, maxBatchSize)}
			currentSizeBytes = batchHeaderSize
		}

		currentBatch.Apuestas = append(currentBatch.Apuestas, *bet)
		currentSizeBytes += estimatedBetSize
		validBetsCount++
	}

	if len(currentBatch.Apuestas) > 0 {
		if err := batchHandler(&currentBatch, true); err != nil {
			return err
		}
	}

	if skippedCount > 0 {
		log.Debugf("action: read_csv | result: partial | client_id: %v | valid_bets: %d | skipped_bets: %d", 
			"streaming", validBetsCount, skippedCount)
	}
	return nil
}
