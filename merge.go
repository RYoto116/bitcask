package bitcask

import (
	"bitcask-kv/data"
	"bitcask-kv/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// 清理无效数据，生成Hint文件
func (db *DB) Merge() error {
	// 如果当前db没有数据，直接返回
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	// 如果merge正在进行，直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgressing
	}

	dirSize, err := utils.DirSize(db.opt.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}

	if float32(db.reclaimSize)/float32(dirSize) < db.opt.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRationUnreached
	}

	// 剩余磁盘空间是否能够容纳 merge 之后的数据集
	availSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}

	if availSize < uint64(dirSize-db.reclaimSize) {
		db.mu.Unlock()
		return ErrNoEnoughSpaceToMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 将当前活跃文件转换为旧的数据文件，并打开新的活跃数据文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// 记录最近没有参与merge的文件ID
	lastNonMergeFileID := db.activeFile.FileID

	// 当前所有旧的数据文件就是需要merge的数据文件
	var mergeFiles []*data.DataFile
	for _, dataFile := range db.olderFiles {
		mergeFiles = append(mergeFiles, dataFile)
	}
	db.mu.Unlock()

	// 对mergeFiles排序，从小到大进行merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	mergePath := db.getMergePath()

	// 如果merge目录存在，说明之前进行过merge，需要将目录删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建一个merge目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeOpt := db.opt
	mergeOpt.DirPath = mergePath
	// 如果merge中途出错，不应该Sync。可以自定义merge的Sync时间
	mergeOpt.SyncWrites = false

	// 打开新的临时bitcask实例用于merge
	mergeDB, err := OpenDB(mergeOpt)
	if err != nil {
		return err
	}

	// 打开Hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return nil
	}

	// 遍历需要merge的文件，取出记录重写有效数据
	for _, dataFile := range mergeFiles {

		var offset int64 = 0

		for {
			logRecord, n, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 解析得到实际key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// 将记录所在文件ID以及offset与索引位置进行比较。如果一致，表示该记录是有效数据，需要重写到merge目录中
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileID && logRecordPos.Offset == offset {
				// 确定有效数据，不需要重写事务序列号
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeqNo(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将当前索引位置写到Hint文件中
				// 格式与数据文件一致，将realKey和pos编码写入hintFile
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += n
		}
	}

	// 将mergeDB的活跃文件和hintFile持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 新增merge完成文件
	mergeFinishedFile, err := data.OpenHintFinishedFile(mergePath)
	if err != nil {
		return err
	}

	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(lastNonMergeFileID))),
	}

	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 获取merge目录
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.opt.DirPath)) // opt.DirPath的父目录
	base := path.Base(db.opt.DirPath)           // opt.DirPath的文件夹名
	return filepath.Join(dir, base+mergeDirName)
}

// 启动数据库时加载merge数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()

	// merge目录不存在，不需要加载直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		// 删除merge目录
		_ = os.RemoveAll(mergePath)
	}()

	entries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识merge完成的文件，判断merge是否处理完成
	var mergeFinished bool = false
	var mergeFileNames []string
	for _, entry := range entries {
		if entry.Name() == data.SeqNoFileName || entry.Name() == fileLockName {
			continue
		}
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !mergeFinished {
		return nil
	}

	// 如果merge完成，删除旧的数据文件，用merge目录的数据文件替代
	// 打开mergeFinished文件，找到最近没有参与merge的文件ID。在该ID之前的文件需要删除
	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileID; fileId++ {
		fileName := data.GetFileName(db.opt.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将merge目录的数据文件移动到数据目录中(包含hint、mergeFinished文件)
	for _, mergeFileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, mergeFileName)
		dstPath := filepath.Join(db.opt.DirPath, mergeFileName)
		// Rename相当于Linux的mv指令
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}

	return nil
}

// 从hint文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	// 查看hint文件是否存在
	if _, err := os.Stat(filepath.Join(db.opt.DirPath, data.HintFileName)); os.IsNotExist(err) {
		return nil
	}

	hintFile, err := data.OpenHintFile(db.opt.DirPath)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		hintRecord, n, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		pos := data.DecodeLogRecordPos(hintRecord.Value)
		db.index.Put(hintRecord.Key, pos)

		offset += n
	}

	return nil
}

func (db *DB) getNonMergeFileID(mergePath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenHintFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}

	mergeFinRecord, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileID, err := strconv.Atoi(string(mergeFinRecord.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileID), nil
}
