package file

import (
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
)

// FileManager handles actual interact with the OS file system.
// It contains the directory of the database, the initailized block size, and how many blocks are used.
type FileManager interface {
	Read(blk *BlockId, page *Page) error
	Write(blk *BlockId, page *Page) error
	Append(filename string) (*BlockId, error)
	Length(filename string) (int, error)
	BlockSize() int
}

var _ FileManager = (*localfm)(nil)

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

	fm := &localfm{
		dirname: dirname,
		blksize: blksize,
		mu:      &sync.RWMutex{},
		files:   make(map[string]*os.File),
	}

	// Set up the finalizer to ensure Close is called when garbage collected
	runtime.SetFinalizer(fm, func(fm *localfm) {
		fm.finalize()
	})

	return fm, nil
}

type localfm struct {
	dirname string
	blksize int

	mu    *sync.RWMutex
	files map[string]*os.File
}

func (fm localfm) Read(blk *BlockId, page *Page) error {
	f, err := fm.open(blk.Filename())
	if err != nil {
		return err
	}

	pos := blk.Number() * fm.blksize
	if _, err := f.Seek(int64(pos), 0); err != nil {
		return ErrFMReadSeek(fm.dirname, blk.Filename(), pos, err)
	}

	if _, err := f.Read(page.buffer); err != nil {
		return ErrFMRead(fm.dirname, blk.Filename(), pos, err)
	}

	return nil
}

func (fm localfm) Write(blk *BlockId, page *Page) error {
	f, err := fm.open(blk.Filename())
	if err != nil {
		return err
	}

	pos := blk.Number() * fm.blksize
	if _, err := f.Seek(int64(pos), 0); err != nil {
		return ErrFMWriteSeek(fm.dirname, blk.Filename(), pos, err)
	}

	if _, err := f.Write(page.buffer); err != nil {
		return ErrFMWrite(fm.dirname, blk.Filename(), pos, err)
	}

	return nil
}

func (fm localfm) Append(filename string) (*BlockId, error) {
	// don't reuse the Length method to avoid the overhead of locking and unlocking
	f, err := fm.open(filename)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, ErrFMAppendStat(fm.dirname, filename, err)
	}
	blknum := int(stat.Size() / int64(fm.blksize))

	blk := NewBlockId(filename, blknum)

	bytes := make([]byte, fm.blksize)
	pos := blk.Number() * fm.blksize
	if _, err := f.Seek(int64(pos), 0); err != nil {
		return nil, ErrFMAppendSeek(fm.dirname, filename, pos, err)
	}

	if _, err := f.Write(bytes); err != nil {
		return nil, ErrFMAppend(fm.dirname, filename, pos, err)
	}

	return blk, nil
}

func (fm localfm) Length(filename string) (int, error) {
	f, err := fm.open(filename)
	if err != nil {
		return 0, err
	}
	stat, err := f.Stat()
	if err != nil {
		return 0, ErrFMLengthStat(fm.dirname, filename, err)
	}

	return int(stat.Size() / int64(fm.blksize)), nil
}

func (fm localfm) BlockSize() int {
	return fm.blksize
}

func (fm localfm) open(filename string) (*os.File, error) {
	fm.mu.RLock()
	f, ok := fm.files[filename]
	fm.mu.RUnlock()

	if ok {
		return f, nil
	}

	// If file isn't open, acquire write lock
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Double-check after acquiring write lock
	if f, ok := fm.files[filename]; ok {
		return f, nil
	}

	filepath := path.Join(fm.dirname, filename)
	// Open with both read and write access along with O_SYNC
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)
	if err != nil {
		return nil, ErrFMCreateOpenFile(filepath, err)
	}
	fm.files[filename] = f
	return f, nil
}

func (fm *localfm) finalize() {
	for filename, f := range fm.files {
		if err := f.Close(); err != nil {
			log.Println(ErrFMFinalize(filename, err).Error())
		}
	}
}
