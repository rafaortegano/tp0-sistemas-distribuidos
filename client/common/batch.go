package common

import (
	"fmt"
)

// Batch represents a collection of bets from a single agency
type Batch struct {
	AgenciaID uint8
	Apuestas  []Bet
	IsLastBatch  bool
}

// NewBatch creates a new batch for the given agency
func NewBatch(agenciaID uint8) *Batch {
	return &Batch{
		AgenciaID: agenciaID,
		Apuestas:  make([]Bet, 0),
		IsLastBatch: false,
	}
}


// Size returns the number of bets in the batch
func (b *Batch) Size() int {
	return len(b.Apuestas)
}

// Validate validates all bets in the batch
func (b *Batch) Validate() error {
	if b.AgenciaID == 0 || b.AgenciaID > 5 {
		return fmt.Errorf("agencia_id must be between 1-5")
	}
	
	if len(b.Apuestas) == 0 {
		return fmt.Errorf("batch must contain at least one bet")
	}
	
	for i, bet := range b.Apuestas {
		if err := bet.Validate(); err != nil {
			return fmt.Errorf("bet %d invalid: %v", i, err)
		}
	}
	
	return nil
}
