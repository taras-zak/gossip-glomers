package crdt

import (
	"maps"
)

type PNCounter struct {
	Positive GCounter
	Negative GCounter
}

func NewPNCounter() PNCounter {
	return PNCounter{
		Positive: make(GCounter),
		Negative: make(GCounter),
	}
}

func (c PNCounter) Increment(key string, delta int) {
	if delta >= 0 {
		c.Positive.Increment(key, delta)
	}
	if delta < 0 {
		c.Negative.Increment(key, -delta)
	}
}

func (c PNCounter) Value() int {
	return c.Positive.Value() - c.Negative.Value()
}

func (c PNCounter) Merge(other PNCounter) {
	c.Positive.Merge(other.Positive)
	c.Negative.Merge(other.Negative)
}

func (c PNCounter) Copy() PNCounter {
	cpy := NewPNCounter()
	maps.Copy(cpy.Positive, c.Positive)
	maps.Copy(cpy.Negative, c.Negative)
	return cpy
}
