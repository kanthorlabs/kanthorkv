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

func NewFileManager(dirname string, blksize int) (FileManager, error) {
	db, err := os.Stat(dirname)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, ErrFMUnknown(dirname, err)
		}

		if err = os.MkdirAll(dirname, 0755); err != nil {
			return nil, ErrFMCreateDir(dirname, err)
		}

		db, _ = os.Stat(dirname)
	}

	if !db.IsDir() {
		return nil, ErrFMIsNotDir(dirname)
	}

	// remove any leftover temporary tables
	files, err := os.ReadDir(dirname)
	if err != nil {
		return nil, ErrFMReadDir(dirname, err)
	}
	for _, file := range files {
		if !strings.HasPrefix(file.Name(), "temp") {
			continue
		}

		if err = os.Remove(file.Name()); err != nil {
			return nil, ErrFMDelTempFile(dirname, file.Name(), err)
		}
	}

	return localfm{
		dirname: dirname,
		blksize: blksize,
		files:   make(map[string]*os.File),
		mus:     make(map[string]*sync.Mutex),
	}, nil
}

type localfm struct {
	dirname string
	blksize int
	files   map[string]*os.File
	mus     map[string]*sync.Mutex
}

func (fm localfm) Read(blk *BlockId, page *Page) error {
	fm.lock(blk.Filename())
	defer fm.unlock(blk.Filename())

	f, err := fm.open(blk.Filename())
	if err != nil {
		return ErrFMReadOpenFile(fm.dirname, blk.Filename(), err)
	}

	pos := int64(blk.Number()) * int64(fm.blksize)
	if _, err := f.Seek(pos, 0); err != nil {
		return ErrFMReadSeek(fm.dirname, blk.Filename(), pos, err)
	}

	if _, err := f.Read(page.buffer); err != nil {
		return ErrFMRead(fm.dirname, blk.Filename(), pos, err)
	}

	return nil
}

func (fm localfm) Write(blk *BlockId, page *Page) error {
	fm.lock(blk.Filename())
	defer fm.unlock(blk.Filename())

	f, err := fm.open(blk.Filename())
	if err != nil {
		return ErrFMWriteOpenFile(fm.dirname, blk.Filename(), err)
	}

	pos := int64(blk.Number()) * int64(fm.blksize)
	if _, err := f.Seek(pos, 0); err != nil {
		return ErrFMWriteSeek(fm.dirname, blk.Filename(), pos, err)
	}

	if _, err := f.Write(page.buffer); err != nil {
		return ErrFMWrite(fm.dirname, blk.Filename(), pos, err)
	}

	return nil
}

func (fm localfm) Append(filename string) (*BlockId, error) {
	// don't reuse the Length method to avoid the overhead of locking and unlocking
	fm.lock(filename)
	defer fm.unlock(filename)

	f, err := fm.open(filename)
	if err != nil {
		return nil, ErrFMAppendOpenFile(fm.dirname, filename, err)
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, ErrFMAppendStat(fm.dirname, filename, err)
	}
	blknum := stat.Size() / int64(fm.blksize)

	blk, err := NewBlockId(filename, blknum)
	if err != nil {
		return nil, ErrFMAppendNewBlock(fm.dirname, filename, blknum, err)
	}

	bytes := make([]byte, fm.blksize)
	pos := int64(blk.Number()) * int64(fm.blksize)
	if _, err := f.Seek(pos, 0); err != nil {
		return nil, ErrFMAppendSeek(fm.dirname, filename, pos, err)
	}

	if _, err := f.Write(bytes); err != nil {
		return nil, ErrFMAppend(fm.dirname, filename, pos, err)
	}

	return blk, nil
}

func (fm localfm) Length(filename string) (int64, error) {
	fm.lock(filename)
	defer fm.unlock(filename)

	f, err := fm.open(filename)
	if err != nil {
		return 0, ErrFMLengthOpenFile(fm.dirname, filename, err)
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, ErrFMLengthStat(fm.dirname, filename, err)
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
	if f, ok := fm.files[filename]; ok {
		return f, nil
	}

	filepath := path.Join(fm.dirname, filename)
	if _, err := os.Stat(filepath); err != nil {
		if !os.IsNotExist(err) {
			return nil, ErrFMUnknown(fm.dirname, err)
		}

		f, err := os.Create(filepath)
		if err != nil {
			return nil, ErrFMCreateFile(filepath, err)
		}

		fm.files[filename] = f
		return f, nil
	}

	f, err = os.OpenFile(path.Join(fm.dirname, filename), os.O_SYNC, 0644)
	if err != nil {
		return nil, err
	}
	fm.files[filename] = f
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
