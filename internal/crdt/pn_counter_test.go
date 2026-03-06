package crdt

import "testing"

func TestPNCounter_Increment(t *testing.T) {
	tests := []struct {
		name string
		ops  []struct {
			key   string
			delta int
		}
		wantValue int
	}{
		{
			name: "single increment",
			ops: []struct {
				key   string
				delta int
			}{{"n0", 5}},
			wantValue: 5,
		},
		{
			name: "single decrement",
			ops: []struct {
				key   string
				delta int
			}{{"n0", -3}},
			wantValue: -3,
		},
		{
			name: "increment then decrement",
			ops: []struct {
				key   string
				delta int
			}{
				{"n0", 5},
				{"n0", -3},
			},
			wantValue: 2,
		},
		{
			name: "decrement below zero",
			ops: []struct {
				key   string
				delta int
			}{
				{"n0", 3},
				{"n0", -5},
			},
			wantValue: -2,
		},
		{
			name: "zero delta is no-op",
			ops: []struct {
				key   string
				delta int
			}{{"n0", 0}},
			wantValue: 0,
		},
		{
			name: "multiple nodes",
			ops: []struct {
				key   string
				delta int
			}{
				{"n0", 5},
				{"n1", 3},
				{"n0", -2},
			},
			wantValue: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewPNCounter()
			for _, op := range tt.ops {
				c.Increment(op.key, op.delta)
			}
			if got := c.Value(); got != tt.wantValue {
				t.Errorf("Value() = %d, want %d", got, tt.wantValue)
			}
		})
	}
}

func TestPNCounter_Value(t *testing.T) {
	tests := []struct {
		name string
		c    PNCounter
		want int
	}{
		{
			name: "empty counter",
			c:    NewPNCounter(),
			want: 0,
		},
		{
			name: "only positive",
			c:    PNCounter{Positive: GCounter{"n0": 10}, Negative: GCounter{}},
			want: 10,
		},
		{
			name: "only negative",
			c:    PNCounter{Positive: GCounter{}, Negative: GCounter{"n0": 4}},
			want: -4,
		},
		{
			name: "positive and negative",
			c:    PNCounter{Positive: GCounter{"n0": 10}, Negative: GCounter{"n0": 4}},
			want: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.Value(); got != tt.want {
				t.Errorf("Value() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPNCounter_Merge(t *testing.T) {
	tests := []struct {
		name      string
		c         PNCounter
		other     PNCounter
		wantValue int
	}{
		{
			name:      "merge disjoint increments",
			c:         PNCounter{Positive: GCounter{"n0": 5}, Negative: GCounter{}},
			other:     PNCounter{Positive: GCounter{"n1": 3}, Negative: GCounter{}},
			wantValue: 8,
		},
		{
			name:      "merge disjoint decrements",
			c:         PNCounter{Positive: GCounter{}, Negative: GCounter{"n0": 2}},
			other:     PNCounter{Positive: GCounter{}, Negative: GCounter{"n1": 3}},
			wantValue: -5,
		},
		{
			name:      "max wins on positive",
			c:         PNCounter{Positive: GCounter{"n0": 10}, Negative: GCounter{}},
			other:     PNCounter{Positive: GCounter{"n0": 6}, Negative: GCounter{}},
			wantValue: 10,
		},
		{
			name:      "max wins on negative",
			c:         PNCounter{Positive: GCounter{}, Negative: GCounter{"n0": 3}},
			other:     PNCounter{Positive: GCounter{}, Negative: GCounter{"n0": 7}},
			wantValue: -7,
		},
		{
			name:      "idempotent — merge with equal state",
			c:         PNCounter{Positive: GCounter{"n0": 5}, Negative: GCounter{"n0": 2}},
			other:     PNCounter{Positive: GCounter{"n0": 5}, Negative: GCounter{"n0": 2}},
			wantValue: 3,
		},
		{
			name:      "merge with empty",
			c:         PNCounter{Positive: GCounter{"n0": 5}, Negative: GCounter{"n0": 2}},
			other:     NewPNCounter(),
			wantValue: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.c.Merge(tt.other)
			if got := tt.c.Value(); got != tt.wantValue {
				t.Errorf("Value() after Merge = %d, want %d", got, tt.wantValue)
			}
		})
	}
}

// Commutativity: merge(a,b) and merge(b,a) produce the same value
func TestPNCounter_Merge_Commutative(t *testing.T) {
	a := PNCounter{Positive: GCounter{"n0": 5, "n1": 2}, Negative: GCounter{"n0": 1}}
	b := PNCounter{Positive: GCounter{"n0": 3, "n1": 7}, Negative: GCounter{"n1": 4}}

	ab := a.Copy()
	ab.Merge(b)

	ba := b.Copy()
	ba.Merge(a)

	if ab.Value() != ba.Value() {
		t.Errorf("commutativity violated: merge(a,b)=%d, merge(b,a)=%d", ab.Value(), ba.Value())
	}
}

func TestPNCounter_Copy(t *testing.T) {
	t.Run("copy has same value", func(t *testing.T) {
		c := NewPNCounter()
		c.Increment("n0", 5)
		c.Increment("n1", -3)
		cpy := c.Copy()
		if got := cpy.Value(); got != c.Value() {
			t.Errorf("Copy().Value() = %d, want %d", got, c.Value())
		}
	})

	t.Run("copy is independent — mutating original does not affect copy", func(t *testing.T) {
		c := NewPNCounter()
		c.Increment("n0", 5)
		cpy := c.Copy()
		c.Increment("n0", 10)
		if got := cpy.Value(); got != 5 {
			t.Errorf("Copy() was affected by mutation of original: got %d, want 5", got)
		}
	})

	t.Run("copy is independent — mutating copy does not affect original", func(t *testing.T) {
		c := NewPNCounter()
		c.Increment("n0", 5)
		cpy := c.Copy()
		cpy.Increment("n0", 10)
		if got := c.Value(); got != 5 {
			t.Errorf("Original was affected by mutation of copy: got %d, want 5", got)
		}
	})
}

// Monotonicity: merging can only move value in the direction of more information
func TestPNCounter_Merge_Monotone(t *testing.T) {
	a := NewPNCounter()
	a.Increment("n0", 10)

	b := NewPNCounter()
	b.Increment("n0", 10)
	b.Increment("n0", -3)

	before := a.Value()
	a.Merge(b)
	after := a.Value()

	if after > before {
		t.Errorf("positive side grew unexpectedly: before=%d, after=%d", before, after)
	}
}
