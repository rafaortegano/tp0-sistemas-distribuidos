package common

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Bet structure that represents a bet
type Bet struct {
	AgenciaID  uint8
	Nombre     string
	Apellido   string
	Documento  uint32
	Nacimiento uint32
	Numero     uint32
}

// NewBetFromEnv creates a Bet structure from environment variables
func NewBetFromEnv() (*Bet, error) {
	bet := &Bet{}
	
	cliIDStr := os.Getenv("CLI_ID")
	if cliIDStr == "" {
		return nil, fmt.Errorf("CLI_ID environment variable is required")
	}
	cliID, err := strconv.ParseUint(cliIDStr, 10, 8)
	if err != nil {
		return nil, fmt.Errorf("invalid CLI_ID: %v", err)
	}
	bet.AgenciaID = uint8(cliID)
	
	bet.Nombre = os.Getenv("NOMBRE")
	if bet.Nombre == "" {
		return nil, fmt.Errorf("NOMBRE environment variable is required")
	}
	
	bet.Apellido = os.Getenv("APELLIDO")
	if bet.Apellido == "" {
		return nil, fmt.Errorf("APELLIDO environment variable is required")
	}

	documentoStr := os.Getenv("DOCUMENTO")
	if documentoStr == "" {
		return nil, fmt.Errorf("DOCUMENTO environment variable is required")
	}
	documento, err := strconv.ParseUint(documentoStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid DOCUMENTO: %v", err)
	}
	bet.Documento = uint32(documento)
	
	nacimientoStr := os.Getenv("NACIMIENTO")
	if nacimientoStr == "" {
		return nil, fmt.Errorf("NACIMIENTO environment variable is required")
	}

	nacimientoFormatted := strings.ReplaceAll(nacimientoStr, "-", "")
	nacimiento, err := strconv.ParseUint(nacimientoFormatted, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid NACIMIENTO format (expected YYYY-MM-DD): %v", err)
	}
	bet.Nacimiento = uint32(nacimiento)
	

	numeroStr := os.Getenv("NUMERO")
	if numeroStr == "" {
		return nil, fmt.Errorf("NUMERO environment variable is required")
	}
	numero, err := strconv.ParseUint(numeroStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid NUMERO: %v", err)
	}
	bet.Numero = uint32(numero)
	
	return bet, nil
}

// Validate checks if the bet data is valid
func (b *Bet) Validate() error {
	if b.AgenciaID == 0 || b.AgenciaID > 5 {
		return fmt.Errorf("agencia_id must be between 1-5")
	}
	
	if len(b.Nombre) == 0 || len(b.Nombre) > 255 {
		return fmt.Errorf("nombre must be 1-255 characters")
	}
	
	if len(b.Apellido) == 0 || len(b.Apellido) > 255 {
		return fmt.Errorf("apellido must be 1-255 characters")
	}
	
	if b.Documento == 0 {
		return fmt.Errorf("documento must be greater than 0")
	}
	

	if b.Nacimiento < 19000101 || b.Nacimiento > 20251231 {
		return fmt.Errorf("nacimiento must be valid YYYYMMDD between 1900-2025")
	}
	

	if b.Numero == 0 {
		return fmt.Errorf("numero must be greater than 0")
	}
	
	return nil
}


