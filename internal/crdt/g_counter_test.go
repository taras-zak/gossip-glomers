package crdt

import "testing"

func TestGCounter_Increment(t *testing.T) {
	type args struct {
		key   string
		delta int
	}
	tests := []struct {
		name      string
		c         GCounter
		args      args
		wantValue int
	}{
		{
			name:      "increment new key",
			c:         GCounter{},
			args:      args{"n0", 5},
			wantValue: 5,
		},
		{
			name:      "increment existing key",
			c:         GCounter{"n0": 5},
			args:      args{"n0", 3},
			wantValue: 8,
		},
		{
			name:      "increment zero",
			c:         GCounter{"n0": 5},
			args:      args{"n0", 0},
			wantValue: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.c.Increment(tt.args.key, tt.args.delta)
			if got := tt.c[tt.args.key]; got != tt.wantValue {
				t.Errorf("got %d, want %d", got, tt.wantValue)
			}
		})
	}
}

func TestGCounter_Value(t *testing.T) {
	tests := []struct {
		name string
		c    GCounter
		want int
	}{
		{
			name: "empty counter",
			c:    GCounter{},
			want: 0,
		},
		{
			name: "single node",
			c:    GCounter{"n0": 7},
			want: 7,
		},
		{
			name: "multiple nodes",
			c:    GCounter{"n0": 5, "n1": 3, "n2": 2},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.Value(); got != tt.want {
				t.Errorf("Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGCounter_Merge(t *testing.T) {
	type args struct {
		other GCounter
	}
	tests := []struct {
		name      string
		c         GCounter
		args      args
		wantValue int
	}{
		{
			name:      "merge disjoint keys",
			c:         GCounter{"n0": 5},
			args:      args{GCounter{"n1": 3}},
			wantValue: 8,
		},
		{
			name:      "max wins — local higher",
			c:         GCounter{"n0": 10},
			args:      args{GCounter{"n0": 6}},
			wantValue: 10,
		},
		{
			name:      "max wins — remote higher",
			c:         GCounter{"n0": 6},
			args:      args{GCounter{"n0": 10}},
			wantValue: 10,
		},
		{
			name:      "idempotent — merge with equal state",
			c:         GCounter{"n0": 5, "n1": 3},
			args:      args{GCounter{"n0": 5, "n1": 3}},
			wantValue: 8,
		},
		{
			name:      "merge with empty",
			c:         GCounter{"n0": 5},
			args:      args{GCounter{}},
			wantValue: 5,
		},
		{
			name:      "monotone — value never decreases",
			c:         GCounter{"n0": 5},
			args:      args{GCounter{"n0": 3}},
			wantValue: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.c.Merge(tt.args.other)
			if got := tt.c.Value(); got != tt.wantValue {
				t.Errorf("Value() after Merge = %d, want %d", got, tt.wantValue)
			}
		})
	}
}

// Commutativity: merge(a,b) and merge(b,a) produce the same value
func TestGCounter_Merge_Commutative(t *testing.T) {
	a := GCounter{"n0": 5, "n1": 2}
	b := GCounter{"n0": 3, "n1": 7}

	ab := GCounter{"n0": 5, "n1": 2}
	ab.Merge(b)

	ba := GCounter{"n0": 3, "n1": 7}
	ba.Merge(a)

	if ab.Value() != ba.Value() {
		t.Errorf("commutativity violated: merge(a,b)=%d, merge(b,a)=%d", ab.Value(), ba.Value())
	}
}
