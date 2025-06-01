package log

import "github.com/kanthorlabs/kanthorkv/file"

func NewLogIterator(fm file.FileManager, blk *file.BlockId) (*LogIterator, error) {
	page := file.NewPage(fm.BlockSize())

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
	fm         file.FileManager
	blk        *file.BlockId
	page       *file.Page
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
		blk := file.NewBlockId(it.blk.Filename(), it.blk.Number()-1)
		if err := it.moveToBlock(blk); err != nil {
			return nil, err
		}
	}

	rec := it.page.Bytes(it.currentpos)
	it.currentpos += file.INT_SIZE + len(rec)
	return rec, nil
}

func (it *LogIterator) moveToBlock(blk *file.BlockId) error {
	if err := it.fm.Read(blk, it.page); err != nil {
		return err
	}

	it.boundary = it.page.Int(0)
	it.currentpos = it.boundary

	return nil
}
