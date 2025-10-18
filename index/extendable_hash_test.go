package index

import (
	"testing"
)

func TestExtendableHashIndex_getDirIndex(t *testing.T) {
	tests := []struct {
		name        string
		globalDepth int
		hashVal     int
		want        int
	}{
		{
			name:        "global depth 1, hash 0",
			globalDepth: 1,
			hashVal:     0,
			want:        0,
		},
		{
			name:        "global depth 1, hash 1",
			globalDepth: 1,
			hashVal:     1,
			want:        1,
		},
		{
			name:        "global depth 1, hash 2",
			globalDepth: 1,
			hashVal:     2,
			want:        0, // 2 & 1 = 0
		},
		{
			name:        "global depth 1, hash 3",
			globalDepth: 1,
			hashVal:     3,
			want:        1, // 3 & 1 = 1
		},
		{
			name:        "global depth 2, hash 5",
			globalDepth: 2,
			hashVal:     5,
			want:        1, // 5 & 3 = 1
		},
		{
			name:        "global depth 3, hash 7",
			globalDepth: 3,
			hashVal:     7,
			want:        7, // 7 & 7 = 7
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ehi := &ExtendableHashIndex{
				globalDepth: tt.globalDepth,
			}
			got := ehi.getDirIndex(tt.hashVal)
			if got != tt.want {
				t.Errorf("getDirIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtendableHashIndex_doubleDirectory(t *testing.T) {
	ehi := &ExtendableHashIndex{
		globalDepth: 1,
		directory:   []int{0, 1},
		localDepths: []int{1, 1},
	}

	ehi.doubleDirectory()

	if ehi.globalDepth != 2 {
		t.Errorf("globalDepth = %v, want 2", ehi.globalDepth)
	}

	wantDir := []int{0, 1, 0, 1}
	if len(ehi.directory) != len(wantDir) {
		t.Errorf("directory length = %v, want %v", len(ehi.directory), len(wantDir))
	}
	for i := range wantDir {
		if ehi.directory[i] != wantDir[i] {
			t.Errorf("directory[%d] = %v, want %v", i, ehi.directory[i], wantDir[i])
		}
	}

	wantDepths := []int{1, 1, 1, 1}
	if len(ehi.localDepths) != len(wantDepths) {
		t.Errorf("localDepths length = %v, want %v", len(ehi.localDepths), len(wantDepths))
	}
	for i := range wantDepths {
		if ehi.localDepths[i] != wantDepths[i] {
			t.Errorf("localDepths[%d] = %v, want %v", i, ehi.localDepths[i], wantDepths[i])
		}
	}
}

func TestExtendableHashIndex_allocateNewBucket(t *testing.T) {
	ehi := &ExtendableHashIndex{
		directory: []int{0, 1, 2, 1},
	}

	got := ehi.allocateNewBucket()
	want := 3 // max is 2, so next should be 3

	if got != want {
		t.Errorf("allocateNewBucket() = %v, want %v", got, want)
	}
}

func TestExtendableHashIndex_updateDirectoryAfterSplit(t *testing.T) {
	ehi := &ExtendableHashIndex{
		globalDepth: 2,
		directory:   []int{0, 0, 0, 0}, // all pointing to bucket 0
		localDepths: []int{1, 1, 1, 1},
	}

	// Split bucket 0 with local depth 1 to create bucket 1
	// With newLocalDepth = 2
	ehi.updateDirectoryAfterSplit(0, 1, 2)

	// After split with depth 2:
	// Entries with rightmost 2 bits >= 2 should point to new bucket
	// Binary: 00 -> 0, 01 -> 0, 10 -> 1, 11 -> 1
	wantDir := []int{0, 0, 1, 1}
	for i := range wantDir {
		if ehi.directory[i] != wantDir[i] {
			t.Errorf("directory[%d] = %v, want %v", i, ehi.directory[i], wantDir[i])
		}
	}

	wantDepths := []int{2, 2, 2, 2}
	for i := range wantDepths {
		if ehi.localDepths[i] != wantDepths[i] {
			t.Errorf("localDepths[%d] = %v, want %v", i, ehi.localDepths[i], wantDepths[i])
		}
	}
}

func TestExtendableHashIndex_SearchCost(t *testing.T) {
	ehi := &ExtendableHashIndex{}

	// Extendable hash should always be 2 (directory + bucket)
	cost := ehi.SearchCost(100, 10)
	if cost != 2 {
		t.Errorf("SearchCost() = %v, want 2", cost)
	}
}
