package index

type Index interface {
	SearchCost(numblocks, rpb int) int
}
