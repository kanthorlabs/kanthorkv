package storage

func NewLogIterator(fm FileManager, blk *BlockId) (*LogIterator, error) {
	page, err := NewPage(fm.BlockSize())
	if err != nil {
		return nil, err
	}

	it := &LogIterator{
		fm:         fm,
		blk:        blk,
		page:       page,
		currentpos: 0,
		boundary:   0,
	}

	if err := it.moveToBlock(blk); err != nil {
		return nil, err
	}

	return it, nil
}

type LogIterator struct {
	fm         FileManager
	blk        *BlockId
	page       *Page
	currentpos int
	boundary   int
}

func (it *LogIterator) HasNext() bool {
	return it.currentpos < it.fm.BlockSize() || it.blk.Number() > 0
}

func (it *LogIterator) Next() ([]byte, error) {
	// at the end of current block
	if it.currentpos == it.fm.BlockSize() {
		// move to previous block to continue reading
		blk, err := NewBlockId(it.blk.Filename(), it.blk.Number()-1)
		if err != nil {
			return nil, err
		}
		if err := it.moveToBlock(blk); err != nil {
			return nil, err
		}
	}

	rec := it.page.Bytes(it.currentpos)
	it.currentpos += INT_SIZE + len(rec)
	return rec, nil
}

func (it *LogIterator) moveToBlock(blk *BlockId) error {
	if err := it.fm.Read(blk, it.page); err != nil {
		return err
	}

	it.boundary = it.page.Int(0)
	it.currentpos = it.boundary

	return nil
}
