package utils

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDirSize(t *testing.T) {
	size, err := DirSize(filepath.Join("/tmp", "dir_test", "0.data"))
	assert.Nil(t, err)
	t.Log(size)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	assert.True(t, size > 0)
}
