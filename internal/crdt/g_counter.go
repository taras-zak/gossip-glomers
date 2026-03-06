package crdt

type GCounter map[string]int

func (c GCounter) Increment(key string, delta int) {
	c[key] += delta
}

func (c GCounter) Value() int {
	sum := 0
	for _, val := range c {
		sum += val
	}
	return sum
}

func (c GCounter) Merge(other GCounter) {
	for key, val := range other {
		c[key] = max(c[key], val)
	}
}
