package tree

import (
	"slices"
)

type Tree struct {
	arr       []string
	branching int
}

func NewTree(nodes []string, branching int) *Tree {
	//slices.Sort(nodes)
	//TODO: unique check
	return &Tree{
		arr:       nodes,
		branching: branching,
	}
}

func (t *Tree) Children(node string) []string {
	nodeIndex := slices.Index(t.arr, node)
	if nodeIndex == -1 {
		panic("node not known")
	}
	firstNeighbour := min(t.branching*nodeIndex+1, len(t.arr))
	lastNeighbour := min(t.branching*nodeIndex+t.branching, len(t.arr)-1)
	return t.arr[firstNeighbour : lastNeighbour+1]
}

func (t *Tree) Parent(node string) string {
	nodeIndex := slices.Index(t.arr, node)
	if nodeIndex == -1 {
		panic("node not known")
	}
	return t.arr[(nodeIndex-1)/t.branching]
}
