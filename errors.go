package bitcask

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("the key is not found in database")
	ErrDataFileNotFound       = errors.New("the data file is not found")
	ErrDataDirectoryCorrupted = errors.New("the data directory maybe corrupted")
	ErrExceedMaxBatchSize     = errors.New("exceed the max batch size")
	ErrMergeIsProgressing     = errors.New("merge is in progress, try again later")
	ErrDatabaseIsUsing        = errors.New("the data directory is used by another process")
	ErrMergeRationUnreached   = errors.New("the merge ratio does not reach the option")
	ErrNoEnoughSpaceToMerge   = errors.New("no enough disk space to merge")
)
