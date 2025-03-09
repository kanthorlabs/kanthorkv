package storage

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
)

// FileManager handles actual interact with the OS file system.
// It contains the directory of the database, the initailized block size, and how many blocks are used.
type FileManager interface {
	Read(blk *BlockId, page *Page) error
	Write(blk *BlockId, page *Page) error
	Append(filename string) (*BlockId, error)
	Length(filename string) (int64, error)
	BlockSize() int
}

func NewFileManager(dbname string, blksize int) (FileManager, error) {
	db, err := os.Stat(dbname)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, ErrFMUnknown(dbname, err)
		}

		if err = os.MkdirAll(dbname, 0644); err != nil {
			return nil, ErrFMCreateDir(dbname, err)
		}

		db, _ = os.Stat(dbname)
	}

	if !db.IsDir() {
		return nil, ErrFMIsNotDir(dbname)
	}

	// remove any leftover temporary tables
	files, err := os.ReadDir(dbname)
	if err != nil {
		return nil, ErrFMReadDir(dbname, err)
	}
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "temp") {
			continue
		}

		if err = os.Remove(file.Name()); err != nil {
			return nil, ErrFMDelTempFile(dbname, file.Name(), err)
		}
	}

	return localfm{
		dbname:  dbname,
		blksize: blksize,
		files:   make(map[string]*os.File),
	}, nil
}

type localfm struct {
	dbname  string
	blksize int
	files   map[string]*os.File
	mus     map[string]*sync.Mutex
}

func (fm localfm) Read(blk *BlockId, page *Page) error {
	fm.lock(blk.Filename())
	defer fm.unlock(blk.Filename())

	f, err := fm.open(blk.Filename())
	if err != nil {
		return ErrFMReadOpenFile(fm.dbname, blk.Filename(), err)
	}

	pos := int64(blk.Number()) * int64(fm.blksize)
	if _, err := f.Seek(pos, 0); err != nil {
		return ErrFMReadSeek(fm.dbname, blk.Filename(), pos, err)
	}

	if _, err := f.Read(page.buffer); err != nil {
		return ErrFMRead(fm.dbname, blk.Filename(), pos, err)
	}

	return nil
}

func (fm localfm) Write(blk *BlockId, page *Page) error {
	fm.lock(blk.Filename())
	defer fm.unlock(blk.Filename())

	f, err := fm.open(blk.Filename())
	if err != nil {
		return ErrFMWriteOpenFile(fm.dbname, blk.Filename(), err)
	}

	pos := int64(blk.Number()) * int64(fm.blksize)
	if _, err := f.Seek(pos, 0); err != nil {
		return ErrFMWriteSeek(fm.dbname, blk.Filename(), pos, err)
	}

	if _, err := f.Write(page.buffer); err != nil {
		return ErrFMWrite(fm.dbname, blk.Filename(), pos, err)
	}

	return nil
}

func (fm localfm) Append(filename string) (*BlockId, error) {
	// don't reuse the Length method to avoid the overhead of locking and unlocking
	fm.lock(filename)
	defer fm.unlock(filename)

	f, err := fm.open(filename)
	if err != nil {
		return nil, ErrFMAppendOpenFile(fm.dbname, filename, err)
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, ErrFMAppendStat(fm.dbname, filename, err)
	}
	blknum := stat.Size() / int64(fm.blksize)

	blk, err := NewBlockId(filename, blknum)
	if err != nil {
		return nil, ErrFMAppendNewBlock(fm.dbname, filename, blknum, err)
	}

	bytes := make([]byte, fm.blksize)
	pos := int64(blk.Number()) * int64(fm.blksize)
	if _, err := f.Seek(pos, 0); err != nil {
		return nil, ErrFMAppendSeek(fm.dbname, filename, pos, err)
	}

	if _, err := f.Write(bytes); err != nil {
		return nil, ErrFMAppend(fm.dbname, filename, pos, err)
	}

	return blk, nil
}

func (fm localfm) Length(filename string) (int64, error) {
	fm.lock(filename)
	defer fm.unlock(filename)

	f, err := fm.open(filename)
	if err != nil {
		return 0, ErrFMLengthOpenFile(fm.dbname, filename, err)
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, ErrFMLengthStat(fm.dbname, filename, err)
	}

	return stat.Size() / int64(fm.blksize), nil
}

func (fm localfm) BlockSize() int {
	return fm.blksize
}

func (fm localfm) lock(filename string) {
	if _, ok := fm.mus[filename]; !ok {
		fm.mus[filename] = &sync.Mutex{}
	}

	fm.mus[filename].Lock()
}

func (fm localfm) unlock(filename string) {
	if _, ok := fm.mus[filename]; !ok {
		panic(fmt.Sprintf("KANTHORKV.STORAGE.FILE_MANAGER.UNLOCK_UNKNOWN_FILE: %s", filename))
	}

	fm.mus[filename].Unlock()
}

func (fm localfm) open(filename string) (f *os.File, err error) {
	f, ok := fm.files[filename]

	if !ok {
		// Open the file in sync mode (equavielent to rws in java)
		// that flag tell the operating system should not delay disk I/O.
		// It ensures that the database engine knows exactly when disk writes occur,
		// which is important for implementing the data recovery algorithms.
		f, err = os.OpenFile(path.Join(fm.dbname, filename), os.O_SYNC, 0644)
		if err != nil {
			return nil, err
		}
		fm.files[filename] = f
	}

	return f, nil
}

func (fm localfm) Close() error {
	var msgs []string

	for filename, f := range fm.files {
		fm.lock(filename)
		if err := f.Close(); err != nil {
			msgs = append(msgs, fmt.Sprintf("%s=%v", filename, err))
		}
	}
	if len(msgs) > 0 {
		return fmt.Errorf("KANTHORKV.STORAGE.FILE_MANAGER.CLOSE: %s", strings.Join(msgs, ", "))
	}

	return nil
}
