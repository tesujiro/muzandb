package storage

import (
	"fmt"
	"os"
)

const BlockSize = 1024

type file struct {
	path   string
	fp     *os.File
	blocks uint32
}

func newFile(path string, size uint32) (*file, error) {
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
	for i := uint32(0); i < blocks; i++ {
		_, err := fp.Write(nullBlock)
		if err != nil {
			return nil, err

		}
	}
	fmt.Printf("init %v bytes.\n", BlockSize*blocks)
	return &file{path: path, fp: fp, blocks: blocks}, nil
}

func (file *file) write(block, byt uint32, buf []byte) error {
	if block > file.blocks {
		return fmt.Errorf("block %v larger than blocks %v", block, file.blocks)
	}
	if byt > BlockSize {
		return fmt.Errorf("byte number %v larger than BlockSize %v", byt, BlockSize)
	}
	// TODO: check len(buf)

	file.fp.Seek(int64(BlockSize*block+byt), os.SEEK_SET)
	_, err := file.fp.Write(buf)
	return err
}

func (file *file) read(block, byt uint32, size int) ([]byte, error) {
	if block > file.blocks {
		return nil, fmt.Errorf("block %v larger than blocks %v", block, file.blocks)
	}
	if byt > BlockSize {
		return nil, fmt.Errorf("byte number %v larger than BlockSize %v", byt, BlockSize)
	}
	// TODO: check size

	file.fp.Seek(int64(BlockSize*block+byt), os.SEEK_SET)
	buf := make([]byte, size)
	_, err := file.fp.Read(buf)
	return buf, err
}

func (file *file) readBlock(block uint32) ([]byte, error) {
	return file.read(block, 0, BlockSize)
}

func (file *file) close() error {
	return file.fp.Close()
}
