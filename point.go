package serial

import (
	"encoding/binary"
	"math"
	"time"
)

// Point is a single point in the database
type Point struct {
	TimeStamp time.Time
	Metric    float64
	Tags      []KeyVal
}

// Size returns the size of a point once it has been encoded as binary
func (p *Point) BinSize() uint64 {
	// time + metric + tag length + (tags)
	return 15 + 8 + 8 + (len(p.Tags) * KeyVal.BinSize())
}

// pointHeader encodes the point to a form which it can be stored in the db
func (p *Point) pointHeader() ([]byte, error) {
	buff := make([]byte, 31)
	tBuff, err := p.TimeStamp.MarshalBinary()
	if err != nil {
		return buff, err
	}

	copy(buff[:15], tBuff)
	binary.LittleEndian.PutUint64(buff[15:23], math.Float64bits(p.Metric))

	// put the size of the tags
	binary.LittleEndian.PutUint64(buff[23:31], uint64(len(p.Tags)))
}
