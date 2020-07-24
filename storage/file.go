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
	nullPage := make([]byte, PageSize)
	for i := uint32(0); i < file.Pages; i++ {
		_, err := fp.Write(nullPage)
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
	//fp, err := os.Open(file.Path)
	fp, err := os.OpenFile(file.Path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	file.fp = fp

	return nil
}

func (file *File) write(page, byt uint32, buf []byte) error {
	if page > file.Pages {
		return fmt.Errorf("page %v larger than pages %v", page, file.Pages)
	}
	if byt > PageSize {
		return fmt.Errorf("byte number %v larger than PageSize %v", byt, PageSize)
	}
	// TODO: check len(buf)

	file.fp.Seek(int64(PageSize*page+byt), os.SEEK_SET)
	_, err := file.fp.Write(buf)
	return err
}

func (file *File) writePage(page uint32, buf []byte) error {
	return file.write(page, 0, buf)
}

func (file *File) read(page, byt uint32, size int) ([]byte, error) {
	if page > file.Pages {
		return nil, fmt.Errorf("page %v larger than pages %v", page, file.Pages)
	}
	if byt > PageSize {
		return nil, fmt.Errorf("byte number %v larger than PageSize %v", byt, PageSize)
	}
	// TODO: check size

	file.fp.Seek(int64(PageSize*page+byt), os.SEEK_SET)
	buf := make([]byte, size)
	_, err := file.fp.Read(buf)
	return buf, err
}

func (file *File) readPage(page uint32) ([]byte, error) {
	return file.read(page, 0, PageSize)
}

func (file *File) close() error {
	return file.fp.Close()
}
