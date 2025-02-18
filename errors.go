package bitcask

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("the key is not found in database")
	ErrDataFileNotFound       = errors.New("the data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the data directory maybe corrupted")
	ErrExceedMaxBatchSize     = errors.New("exceed the max batch size")
)
