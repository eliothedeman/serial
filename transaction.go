package serial

import "bytes"

// Transaction provides methods for rolling back writes
type Transaction struct {
	ptrStore, blockStore *bytes.Buffer // in memory forward buffers
	Table                *Table
}

// NewTransaction create and return a new transaction
func NewTransaction(Table *Table) *Transaction {
	return &Transaction{
		Table:      Table,
		ptrStore:   bytes.NewBuffer(nil),
		blockStore: bytes.NewBuffer(nil),
	}
}

func (t *Transaction) name() {

}
