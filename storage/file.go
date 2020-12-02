package storage

import (
	"fmt"
	"os"
)

type File struct {
	FID     FID
	Path    string
	Size    uint32
	Pages   uint32
	CurPage uint32
	fp      *os.File
}

func newFile(fid FID, path string, size uint32) *File {
	pages := size / PageSize
	return &File{FID: fid, Path: path, Size: PageSize * pages, Pages: pages, CurPage: 0}
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
	if page > file.CurPage {
		return fmt.Errorf("page %v larger than used pages %v", page, file.CurPage)
	}
	if byt > PageSize {
		return fmt.Errorf("byte number %v larger than PageSize %v", byt, PageSize)
	}
	// TODO: check len(buf)

	file.fp.Seek(int64(PageSize*page+byt), os.SEEK_SET)
	_, err := file.fp.Write(buf)
	return err
}

func (file *File) writePage(page *Page, data []byte) error {
	return file.write(page.pagenum, 0, data)
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
	if err != nil {
		fmt.Printf("Error in file.read: %T %v page: %v byt: %v size: %v buf:%v\n", err, err, page, byt, size, buf)
	}
	//fmt.Printf("Normal in file.read: page: %v byt: %v size: %v \n", page, byt, size)
	return buf, err
}

func (file *File) readPage(pagenum uint32) (*Page, error) {
	data, err := file.read(pagenum, 0, PageSize)
	if err != nil {
		return nil, err
	}
	return newPage(file, pagenum, data), nil
}

func (file *File) newPage() (*Page, error) {
	//fmt.Printf("newPage() file:%v\n", file)
	pagenum := file.CurPage
	file.CurPage++
	data, err := file.read(pagenum, 0, PageSize)
	if err != nil {
		return nil, err
	}
	return newPage(file, pagenum, data), nil
}

func (file *File) close() error {
	return file.fp.Close()
}
