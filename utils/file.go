package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
	var size int64 = 0
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			size += info.Size()
		}

		return nil
	})

	return size, err
}

// 获取磁盘剩余可用空间大小
func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}

	return stat.Bavail * uint64(stat.Bsize), nil
}

// 数据备份——拷贝数据目录
func CopyDir(src, dst string, exclude []string) error {
	// 如果目标文件夹不存在，创建对应目录
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		if err := os.MkdirAll(dst, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1) // 取出文件名
		if fileName == "" {
			return nil
		}

		// 检查遍历的当前文件是否需要排除
		for _, e := range exclude {
			matched, err := filepath.Match(info.Name(), e)
			if err != nil {
				return err
			}

			if matched {
				return nil // 排除，跳过
			}
		}

		// 如果拷贝的对象是文件夹，需要在目标目录创建同样的文件夹
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, fileName), os.ModePerm)
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}

		return os.WriteFile(filepath.Join(dst, fileName), data, info.Mode())
	})

}
