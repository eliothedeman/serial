package serial

import (
	"sync/atomic"
)

// TODO: Transaction spesific functions for View and Update.

// A DB is a transactional and persistant trie based key value store.
type DB struct {

	// The storage interface of the database.
	s Storage

	// The current transaction id
	transactionCount uint64

	// The root of the trie.
	root *atomic.Value
}

// View calls the given function with a read only transaction that gives a view of the database
// at the time View is called.
func (d *DB) View(f func(Tx) error) error {
	t := &ROTx{
		s:  d.s,
		r:  d.getRoot(),
		id: d.NextTransactionID(),
	}

	err := f(t)
	if err != nil {
		return err
	}

	return t.err
}

// Update calls the given function with a read write transaction that give s view of the database
// at the time Update is called.
func (d *DB) Update(f func(Tx) error) error {
	r := d.getRoot()
	y := *r
	r = &y

	t := &RWTx{
		s:  d.s,
		r:  r,
		id: d.NextTransactionID(),
	}

	err := f(t)
	if err != nil {
		return err
	}

	d.setRoot(r)
	return t.err
}

// CurrentTransactionID returns the id of the curren transaction.
func (d *DB) CurrentTransactionID() uint64 {
	return atomic.LoadUint64(&d.transactionCount)
}

// NextTransactionID increments the id of the current transaction and returns it.
func (d *DB) NextTransactionID() uint64 {
	return atomic.AddUint64(&d.transactionCount, 1)
}

func (d *DB) getRoot() *Node {
	return d.root.Load().(*Node)
}

func (d *DB) setRoot(n *Node) {
	d.root.Store(n)
}
