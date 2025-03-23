package bitcask

import (
	"bitcask-kv/data"
	"bitcask-kv/fio"
	"bitcask-kv/index"
	"bitcask-kv/utils"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// 存储引擎实例
type DB struct {
	opt             Options
	mu              *sync.RWMutex
	fileIds         []int                     // 仅用于加载索引
	activeFile      *data.DataFile            // 当前活跃文件，用于写入
	olderFiles      map[uint32]*data.DataFile // 旧的数据文件，只用于读
	index           index.Indexer
	seqNo           uint64       // 事务序列号，全局递增
	isMerging       bool         // 数据库是否正在执行merge操作
	seqNoFileExists bool         // 存储事务序列号的文件是否存在
	isInitial       bool         // 是否第一次初始化数据目录
	fileLock        *flock.Flock // 文件锁，保证数据目录只被单进程使用
	bytesWrite      uint         // 当前活跃文件的累计写入字节数
	reclaimSize     int64        // 表示有多少数据是无效的
}

// 存储引擎统计信息
type Stat struct {
	KeyNum      uint  // key总数
	DataFileNum uint  // 数据文件数量
	ReclaimSize int64 // 可以进行merge回收的数据量（B）
	DiskSize    int64 // 数据目录所占磁盘空间大小
}

// 打开存储引擎实例
func OpenDB(opt Options) (*DB, error) {
	// 校验用户配置项
	if err := checkOptions(opt); err != nil {
		return nil, err
	}

	var isInitial bool = false
	// 检查数据目录是否存在
	if _, err := os.Stat(opt.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(opt.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 判断数据目录是否正在被使用
	fileLock := flock.New(filepath.Join(opt.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(opt.DirPath)
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		isInitial = true
	}

	// 初始化DB实例结构体
	db := &DB{
		opt:        opt,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(opt.IndexType, opt.DirPath, opt.SyncWrites),
		isMerging:  false,
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	// 加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// B+树不需要从数据文件中加载索引
	if opt.IndexType != BPlusTree {
		// 从hint文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		// 从未merge的数据文件中加载索引，取得事务序列号
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	} else {
		// 加载事务序列号
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}

		// 手动更新活跃文件偏移量
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	if err := db.resetIoType(); err != nil {
		return nil, err
	}

	return db, nil
}

// 返回数据库相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.Lock()
	defer db.mu.Unlock()

	var dataFileNum = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFileNum++
	}

	diskSize, err := utils.DirSize(db.opt.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size: %v", err))
	}

	return &Stat{
		KeyNum:      uint(db.index.Size()),
		DataFileNum: dataFileNum,
		ReclaimSize: db.reclaimSize,
		DiskSize:    diskSize,
	}
}

// 将数据文件的IOManager设置为标准文件IO
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIoManager(db.opt.DirPath, fio.StandardFIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIoManager(db.opt.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) loadSeqNo() error {
	seqNoPath := filepath.Join(db.opt.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(seqNoPath); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.opt.DirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = uint64(seqNo)
	db.seqNoFileExists = true

	// 重要！！否则一直追加写seqNoFile记录
	return os.Remove(seqNoPath)
}

// 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) BackUp(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return utils.CopyDir(db.opt.DirPath, dir, []string{fileLockName}) // 排除文件锁
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return err
	}

	// 更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	pos := db.index.Get(key)
	return db.getValueByPosition(pos)
}

// 获取数据库中所有key
func (db *DB) ListKeys() [][]byte {
	it := db.index.Iterator(false)
	defer it.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for it.Rewind(); it.Valid(); it.Next() {
		keys[idx] = it.Key()
		idx++
	}
	return keys
}

// 获取所有数据，并执行用户指定操作，直到操作返回false推出循环
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	it := db.index.Iterator(false)
	defer it.Close() // B+树读写事务之间是互斥的
	for it.Rewind(); it.Valid(); it.Next() {
		val, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}
		if !fn(it.Key(), val) {
			break
		}
	}
	return nil
}

// 关闭数据库
func (db *DB) Close() error {
	defer func() {
		// 解开文件锁
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock data directory, %v", err))
		}
	}()

	// 关闭活跃文件
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭B+树索引，防止阻塞
	if err := db.index.Close(); err != nil {
		return err
	}

	// 为了BPlusTree，保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.opt.DirPath)
	if err != nil {
		return err
	}

	logRecord := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	// 写入序列号
	encRecord, _ := data.EncodeLogRecord(logRecord)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭旧的数据文件
	for _, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}

	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	if pos != nil {
		db.reclaimSize += int64(pos.Size)
	}

	return nil
}

func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	// key不存在
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	var dataFile *data.DataFile
	if pos.Fid == db.activeFile.FileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(lr *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(lr)
}

// 将日志记录追加到当前活跃文件
func (db *DB) appendLogRecord(lr *data.LogRecord) (*data.LogRecordPos, error) {
	// 判断当前活跃数据文件是否存在，不存在则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 将LogRecord编码为字节数组
	encRecord, size := data.EncodeLogRecord(lr)

	// 如果待写入数据大小超过活跃文件可写空间，需要将活跃文件持久化并打开新的数据文件
	if size+db.activeFile.WriteOff > db.opt.DataFileSize {
		// 持久化数据文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转化为旧数据文件
		db.olderFiles[db.activeFile.FileID] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置决定是否持久化
	db.bytesWrite += uint(size)
	needSync := db.opt.SyncWrites
	if !needSync {
		if db.opt.BytesPerSync > 0 && db.bytesWrite >= db.opt.BytesPerSync {
			needSync = true
		}
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOff,
		Size:   uint32(size),
	}

	return pos, nil
}

// 设置当前活跃文件
// 访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		db.olderFiles[db.activeFile.FileID] = db.activeFile
		initialFileID = db.activeFile.FileID + 1
	}

	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.opt.DirPath, initialFileID, fio.StandardFIO)
	if err != nil {
		return err
	}

	db.activeFile = dataFile
	return nil
}

func checkOptions(opt Options) error {
	if opt.DirPath == "" {
		return errors.New("data base directory path cannot be empty")
	}

	if opt.DataFileSize <= 0 {
		return errors.New("data file size must be greater than 0")
	}

	if opt.DataFileMergeRatio < 0 || opt.DataFileMergeRatio > 1 {
		return errors.New("invlaie data file merge, must between 0 and 1")
	}

	return nil
}

func (db *DB) loadDataFiles() error {
	// 根据配置项将目录中的数据文件都读取出来
	dirEntries, err := os.ReadDir(db.opt.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历找到以 ".data"结尾的数据文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件ID进行排序（升序）
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历文件ID，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.opt.MMapAtStartUp {
			ioType = fio.MemoryMap
		}

		dataFile, err := data.OpenDataFile(db.opt.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 遍历文件所有记录，更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 数据库为空
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)

	mergeFinFileName := filepath.Join(db.opt.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		hasMerge = true
		nonMergeFileId, err = db.getNonMergeFileID(db.opt.DirPath)
		if err != nil {
			return err
		}
	}

	updateIndex := func(key []byte, typ data.LogRecordType, logRecordPos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		// 重要：判断记录是否被删除
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(logRecordPos.Size)
		} else {
			oldPos = db.index.Put(key, logRecordPos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 暂存事务数据，包含事务ID、记录和内存索引，直到ReadLogRecord读到事务完成记录
	txnRecords := make(map[uint64][]*data.TxnRecord)
	// 便于在加载数据文件时获得最新的序列号
	var currentSeqNo = nonTransactionSeqNo

	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile

		// 如果索引已经从hint文件中加载过，在这里可以跳过
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		if fileId == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			logRecordPos := &data.LogRecordPos{
				Fid:    dataFile.FileID,
				Offset: offset,
				Size:   uint32(size),
			}

			// 解析日志记录是否是通过事务写入的，取得事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)

			// 更新内存索引
			if seqNo == nonTransactionSeqNo {
				// 不是事务数据，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 识别到事务提交标识，更新内存索引
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range txnRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					// 重要：删除已提交事务数据
					delete(txnRecords, seqNo)
				} else {
					logRecord.Key = realKey
					txnRecords[seqNo] = append(txnRecords[seqNo], &data.TxnRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 重要：如果读取到事务记录，更新当前事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size
		}

		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 重要：更新数据库的全局事务序列号
	db.seqNo = currentSeqNo

	return nil
}
