package tree

import (
	"reflect"
	"testing"
)

func TestTree_Children(t *testing.T) {
	type fields struct {
		arr       []string
		branching int
	}
	type args struct {
		node string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{
			name: "tree1, root neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 1,
			},
			args: args{node: "n0"},
			want: []string{"n1"},
		},
		{
			name: "tree1, middle node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 1,
			},
			args: args{node: "n1"},
			want: []string{"n2"},
		},
		{
			name: "tree1, last node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 1,
			},
			args: args{node: "n3"},
			want: []string{},
		},
		{
			name: "tree2, root node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 2,
			},
			args: args{node: "n0"},
			want: []string{"n1", "n2"},
		},
		{
			name: "tree2, middle node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 2,
			},
			args: args{node: "n1"},
			want: []string{"n3"},
		},
		{
			name: "tree2, leaf node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 2,
			},
			args: args{node: "n2"},
			want: []string{},
		},
		{
			name: "tree2, leaf node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 2,
			},
			args: args{node: "n3"},
			want: []string{},
		},
		{
			name: "tree3, root node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3", "n4"},
				branching: 3,
			},
			args: args{node: "n0"},
			want: []string{"n1", "n2", "n3"},
		},
		{
			name: "tree3, middle node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3", "n4", "n5"},
				branching: 3,
			},
			args: args{node: "n1"},
			want: []string{"n4", "n5"},
		},
		{
			name: "tree3, leaf node neighbours",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3", "n4", "n5"},
				branching: 3,
			},
			args: args{node: "n2"},
			want: []string{},
		},
		{
			name: "sandbox",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "n10", "n11", "n12", "n13", "n14", "n15", "n16", "n17", "n18", "n19", "n20", "n21", "n22", "n23", "n24"},
				branching: 2,
			},
			args: args{node: "n0"},
			want: []string{"n1", "n2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := NewTree(tt.fields.arr, tt.fields.branching)
			if got := tree.Children(tt.args.node); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Children() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTree_Parent(t *testing.T) {
	type fields struct {
		arr       []string
		branching int
	}
	type args struct {
		node string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name: "tree2, root parent",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 2,
			},
			args: args{node: "n0"},
			want: "n0",
		},
		{
			name: "tree2, mid node parent",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 2,
			},
			args: args{node: "n1"},
			want: "n0",
		},
		{
			name: "tree2, leaf parent",
			fields: fields{
				arr:       []string{"n0", "n1", "n2", "n3"},
				branching: 2,
			},
			args: args{node: "n3"},
			want: "n1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := NewTree(tt.fields.arr, tt.fields.branching)
			if got := tree.Parent(tt.args.node); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parent() = %v, want %v", got, tt.want)
			}
		})
	}
}
