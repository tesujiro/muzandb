package storage

import (
	"fmt"
	"os"
)

type File struct {
	FID   FID
	Path  string
	Size  uint32
	Pages uint32
	fp    *os.File
}

func newFile(fid FID, path string, size uint32) *File {
	pages := size / PageSize
	return &File{FID: fid, Path: path, Size: PageSize * pages, Pages: pages}
}

func (file *File) create() error {
	if _, err := os.Stat(file.Path); err == nil {
		return fmt.Errorf("file %s already exists.", file.Path)
	} else if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	fp, err := os.Create(file.Path)
	if err != nil {
		fmt.Println(err)
		return err
	}
	nullBlock := make([]byte, PageSize)
	for i := uint32(0); i < file.Pages; i++ {
		_, err := fp.Write(nullBlock)
		if err != nil {
			return err

		}
	}
	file.fp = fp
	fmt.Printf("init %v bytes.\n", PageSize*file.Pages)
	return nil
}

func (file *File) open() error {
	_, err := os.Stat(file.Path)
	if err != nil {
		return err
	}
	fp, err := os.Open(file.Path)
	if err != nil {
		return err
	}
	file.fp = fp

	return nil
}

func (file *File) write(block, byt uint32, buf []byte) error {
	if block > file.Pages {
		return fmt.Errorf("block %v larger than pages %v", block, file.Pages)
	}
	if byt > PageSize {
		return fmt.Errorf("byte number %v larger than PageSize %v", byt, PageSize)
	}
	// TODO: check len(buf)

	file.fp.Seek(int64(PageSize*block+byt), os.SEEK_SET)
	_, err := file.fp.Write(buf)
	return err
}

func (file *File) writeBlock(block uint32, buf []byte) error {
	return file.write(block, 0, buf)
}

func (file *File) read(block, byt uint32, size int) ([]byte, error) {
	if block > file.Pages {
		return nil, fmt.Errorf("block %v larger than pages %v", block, file.Pages)
	}
	if byt > PageSize {
		return nil, fmt.Errorf("byte number %v larger than PageSize %v", byt, PageSize)
	}
	// TODO: check size

	file.fp.Seek(int64(PageSize*block+byt), os.SEEK_SET)
	buf := make([]byte, size)
	_, err := file.fp.Read(buf)
	return buf, err
}

func (file *File) readBlock(block uint32) ([]byte, error) {
	return file.read(block, 0, PageSize)
}

func (file *File) close() error {
	return file.fp.Close()
}
