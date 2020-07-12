package storage

import (
	"fmt"
	"os"
)

const BlockSize = 1024

type file struct {
	path   string
	writer *os.File
}

func newFile(path string, size int) (*file, error) {
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("file %s already exists.", path)
	} else if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	fp, err := os.Create(path)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	blocks := size / BlockSize
	nullBlock := make([]byte, BlockSize)
	for i := 0; i < blocks; i++ {
		_, err := fp.Write(nullBlock)
		if err != nil {
			return nil, err

		}
	}
	fmt.Printf("init %v bytes.\n", BlockSize*blocks)
	return &file{path: path, writer: fp}, nil
}
