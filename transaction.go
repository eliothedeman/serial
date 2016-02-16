package serial

import "bytes"

// Transaction provides methods for rolling back writes
type Transaction struct {
	ptrStore, blockStore *bytes.Buffer // in memory forward buffers
	db                   *DB
}

// NewTransaction create and return a new transaction
func NewTransaction(db *DB) *Transaction {
	return &Transaction{
		db:         db,
		ptrStore:   bytes.NewBuffer(nil),
		blockStore: bytes.NewBuffer(nil),
	}
}

func (t *Transaction) name() {

}
