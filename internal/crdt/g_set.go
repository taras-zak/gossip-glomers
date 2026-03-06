package crdt

type GSet map[int]struct{}

func (c GSet) Add(element int) {
	c[element] = struct{}{}
}

func (c GSet) Values() []int {
	elements := make([]int, 0, len(c))
	for k := range c {
		elements = append(elements, k)
	}
	return elements
}

func (c GSet) Merge(other GSet) {
	for key, val := range other {
		c[key] = val
	}
}
