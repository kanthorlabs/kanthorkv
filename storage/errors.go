package storage

import (
	"fmt"
	"strings"
)

var basename = "KANTHORKV.STORAGE"

func Errf(err string, args ...string) error {
	return fmt.Errorf("%s.%s: %s", basename, err, strings.Join(args, " | "))
}

func ErrBlockIdFilenameEmpty() error {
	return Errf("BLOCK_ID.FILENAME_EMPTY")
}

func ErrBlockIdInvalidBlockNumber(blknum int64) error {
	args := []string{
		fmt.Sprintf("blknum=%d", blknum),
	}
	return Errf("BLOCK_ID.INVALID_BLOCK_NUMBER", args...)
}

func ErrPageInvalidBlockSize(blksize int) error {
	args := []string{
		fmt.Sprintf("blksize=%d", blksize),
	}
	return Errf("PAGE.INVALID_BLOCK_SIZE", args...)
}

func ErrPageSetIntBufferOverflow(offset, valueLen, bufferLen int) error {
	args := []string{
		fmt.Sprintf("offset=%d", offset),
		fmt.Sprintf("value_len=%d", valueLen),
		fmt.Sprintf("buffer_len=%d", bufferLen),
	}
	return Errf("PAGE.SET_INT.BUFFER_OVERFLOW", args...)
}

func ErrPageSetBytesBufferOverflow(offset, valueLen, bufferLen int) error {
	args := []string{
		fmt.Sprintf("offset=%d", offset),
		fmt.Sprintf("value_len=%d", valueLen),
		fmt.Sprintf("buffer_len=%d", bufferLen),
	}
	return Errf("PAGE.SET_BYTES.BUFFER_OVERFLOW", args...)
}

func ErrFMUnknown(dirname string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.DB.STAT", args...)
}

func ErrFMCreateDir(dirname string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.DB.CREATE_DIR", args...)
}

func ErrFMCreateFile(filepath string, err error) error {
	args := []string{
		fmt.Sprintf("filepath=%s", filepath),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.DB.CREATE_FILE", args...)
}

func ErrFMIsNotDir(dirname string) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
	}
	return Errf("FILE_MANAGER.DB.CREATE_DIR", args...)
}

func ErrFMReadDir(dirname string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.DB.READ_DIR", args...)
}

func ErrFMDelTempFile(dirname, filename string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.DB.DEL_TEMP_FILE", args...)
}

func ErrFMReadOpenFile(dirname, filename string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.READ.OPEN_FILE", args...)
}

func ErrFMReadSeek(dirname, filename string, pos int64, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("pos=%d", pos),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.READ.SEEK", args...)
}

func ErrFMRead(dirname, filename string, pos int64, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("pos=%d", pos),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.READ", args...)
}

func ErrFMWriteOpenFile(dirname, filename string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.WRITE.OPEN_FILE", args...)
}

func ErrFMWriteSeek(dirname, filename string, pos int64, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("pos=%d", pos),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.WRITE.SEEK", args...)
}

func ErrFMWrite(dirname, filename string, pos int64, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("pos=%d", pos),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.WRITE", args...)
}

func ErrFMAppendOpenFile(dirname, filename string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.APPEND.OPEN_FILE", args...)
}

func ErrFMAppendStat(dirname, filename string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.APPEND.STAT", args...)
}

func ErrFMAppendNewBlock(dirname, filename string, blknum int64, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("blknum=%d", blknum),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.APPEND.STAT", args...)
}

func ErrFMAppendSeek(dirname, filename string, pos int64, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("pos=%d", pos),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.APPEND.SEEK", args...)
}

func ErrFMAppend(dirname, filename string, pos int64, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("pos=%d", pos),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.APPEND", args...)
}

func ErrFMLengthOpenFile(dirname, filename string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.LENGTH.OPEN_FILE", args...)
}

func ErrFMLengthStat(dirname, filename string, err error) error {
	args := []string{
		fmt.Sprintf("dirname=%s", dirname),
		fmt.Sprintf("filename=%s", filename),
		fmt.Sprintf("err=%v", err),
	}
	return Errf("FILE_MANAGER.LENGTH.STAT", args...)
}
