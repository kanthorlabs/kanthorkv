package log

import "github.com/kanthorlabs/kanthorkv/file"

var _ LogManager = (*locallm)(nil)

type LogManager interface {
	Append(rec []byte) (int, error)
	Flush(lsn int) error
	Iterator() (*LogIterator, error)
}

func NewLogManager(fm file.FileManager, logfile string) (LogManager, error) {
	logpage := file.NewPage(fm.BlockSize())
	logsize, err := fm.Length(logfile)
	if err != nil {
		return nil, err
	}

	lm := &locallm{
		fm:             fm,
		logfile:        logfile,
		logpage:        logpage,
		latestLSN:      0,
		latestSavedLSN: 0,
	}

	if logsize == 0 {
		lm.currentblk, err = lm.appendBlk()
		if err != nil {
			return nil, err
		}
	} else {
		lm.currentblk = file.NewBlockId(logfile, logsize-1)
		if err := fm.Read(lm.currentblk, lm.logpage); err != nil {
			return nil, err
		}
	}
	return lm, nil
}

type locallm struct {
	fm             file.FileManager
	logfile        string
	logpage        *file.Page
	currentblk     *file.BlockId
	latestLSN      int
	latestSavedLSN int
}

func (lm *locallm) Append(rec []byte) (int, error) {
	boundary := lm.logpage.Int(0)
	recsize := len(rec)
	bytesneeded := file.INT_SIZE + recsize

	// it does not fit
	if boundary-bytesneeded < file.INT_SIZE {
		// so that we move to the next block
		lm.flush()

		currentblk, err := lm.appendBlk()
		if err != nil {
			return 0, err
		}
		lm.currentblk = currentblk
		boundary = lm.logpage.Int(0)
	}

	recpos := boundary - bytesneeded
	lm.logpage.SetBytes(recpos, rec)
	lm.logpage.SetInt(0, recpos)
	lm.latestLSN += 1
	return lm.latestLSN, nil
}

func (lm *locallm) Flush(lsn int) error {
	if lsn >= lm.latestSavedLSN {
		return lm.flush()
	}
	return nil
}

func (lm *locallm) Iterator() (*LogIterator, error) {
	if err := lm.flush(); err != nil {
		return nil, err
	}
	return NewLogIterator(lm.fm, lm.currentblk)
}

func (lm *locallm) flush() error {
	if err := lm.fm.Write(lm.currentblk, lm.logpage); err != nil {
		return err
	}

	lm.latestSavedLSN = lm.latestLSN
	return nil
}

func (lm *locallm) appendBlk() (*file.BlockId, error) {
	blk, err := lm.fm.Append(lm.logfile)
	if err != nil {
		return nil, err
	}

	if err := lm.logpage.SetInt(0, lm.fm.BlockSize()); err != nil {
		return nil, err
	}
	if err := lm.fm.Write(blk, lm.logpage); err != nil {
		return nil, err
	}

	return blk, nil
}
