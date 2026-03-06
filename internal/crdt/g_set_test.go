package crdt

import (
	"slices"
	"testing"
)

func TestGSet_Add(t *testing.T) {
	tests := []struct {
		name    string
		c       GSet
		element int
		wantLen int
	}{
		{
			name:    "add to empty set",
			c:       GSet{},
			element: 1,
			wantLen: 1,
		},
		{
			name:    "add duplicate — no growth",
			c:       GSet{1: {}},
			element: 1,
			wantLen: 1,
		},
		{
			name:    "add distinct element",
			c:       GSet{1: {}},
			element: 2,
			wantLen: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.c.Add(tt.element)
			if got := len(tt.c); got != tt.wantLen {
				t.Errorf("len = %d, want %d", got, tt.wantLen)
			}
			if _, ok := tt.c[tt.element]; !ok {
				t.Errorf("element %d not found in set", tt.element)
			}
		})
	}
}

func TestGSet_Values(t *testing.T) {
	tests := []struct {
		name string
		c    GSet
		want []int
	}{
		{
			name: "empty set",
			c:    GSet{},
			want: []int{},
		},
		{
			name: "single element",
			c:    GSet{42: {}},
			want: []int{42},
		},
		{
			name: "multiple elements",
			c:    GSet{1: {}, 2: {}, 3: {}},
			want: []int{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.c.Values()
			slices.Sort(got)
			slices.Sort(tt.want)
			if !slices.Equal(got, tt.want) {
				t.Errorf("Values() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGSet_Merge(t *testing.T) {
	tests := []struct {
		name     string
		c        GSet
		other    GSet
		wantKeys []int
	}{
		{
			name:     "merge disjoint sets",
			c:        GSet{1: {}},
			other:    GSet{2: {}},
			wantKeys: []int{1, 2},
		},
		{
			name:     "merge overlapping sets",
			c:        GSet{1: {}, 2: {}},
			other:    GSet{2: {}, 3: {}},
			wantKeys: []int{1, 2, 3},
		},
		{
			name:     "idempotent — merge with equal state",
			c:        GSet{1: {}, 2: {}},
			other:    GSet{1: {}, 2: {}},
			wantKeys: []int{1, 2},
		},
		{
			name:     "merge with empty",
			c:        GSet{1: {}},
			other:    GSet{},
			wantKeys: []int{1},
		},
		{
			name:     "merge into empty",
			c:        GSet{},
			other:    GSet{1: {}, 2: {}},
			wantKeys: []int{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.c.Merge(tt.other)
			got := tt.c.Values()
			slices.Sort(got)
			slices.Sort(tt.wantKeys)
			if !slices.Equal(got, tt.wantKeys) {
				t.Errorf("Values() after Merge = %v, want %v", got, tt.wantKeys)
			}
		})
	}
}

// Commutativity: merge(a,b) and merge(b,a) contain the same elements
func TestGSet_Merge_Commutative(t *testing.T) {
	a := GSet{1: {}, 2: {}}
	b := GSet{2: {}, 3: {}}

	ab := GSet{1: {}, 2: {}}
	ab.Merge(b)

	ba := GSet{2: {}, 3: {}}
	ba.Merge(a)

	gotAB := ab.Values()
	gotBA := ba.Values()
	slices.Sort(gotAB)
	slices.Sort(gotBA)

	if !slices.Equal(gotAB, gotBA) {
		t.Errorf("commutativity violated: merge(a,b)=%v, merge(b,a)=%v", gotAB, gotBA)
	}
}