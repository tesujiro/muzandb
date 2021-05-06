package page

import (
	"fmt"

	"github.com/tesujiro/muzandb/storage/fio"
)

type PageFile struct {
	*fio.File
	FID     FID
	Size    uint32
	Pages   uint32
	CurPage uint32
}

func NewPageFile(fid FID, path string, size uint32) *PageFile {
	pages := size / PageSize
	return &PageFile{
		FID:     fid,
		File:    &fio.File{Path: path},
		Size:    PageSize * pages,
		Pages:   pages,
		CurPage: 0,
	}
}

func (file *PageFile) Create() error {
	err := file.File.Create()
	if err != nil {
		fmt.Println(err)
		return err
	}

	nullPage := make([]byte, PageSize)
	for i := uint32(0); i < file.Pages; i++ {
		err := file.File.Write(int64(i*PageSize), nullPage)
		if err != nil {
			return err

		}
	}
	//file.File.fp = fp
	fmt.Printf("init %v bytes.\n", PageSize*file.Pages)
	return nil
}

func (file *PageFile) writePage(page *Page, data []byte) error {
	return file.File.Write(int64(PageSize*page.Pagenum), data)
}

func (file *PageFile) Write(page, byt uint32, buf []byte) error {
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

	/*
		file.fp.Seek(int64(PageSize*page+byt), os.SEEK_SET)
		_, err := file.fp.Write(buf)
		return err
	*/
	return file.File.Write(int64(PageSize*page+byt), buf)
}

func (file *PageFile) readPage(pagenum uint32) (*Page, error) {
	data, err := file.File.Read(int64(PageSize*pagenum), PageSize)
	if err != nil {
		return nil, err
	}
	return newPage(file, pagenum, data), nil
}

func (file *PageFile) Read(page, byt uint32, size int) ([]byte, error) {
	if page > file.Pages {
		return nil, fmt.Errorf("page %v larger than pages %v", page, file.Pages)
	}
	if byt > PageSize {
		return nil, fmt.Errorf("byte number %v larger than PageSize %v", byt, PageSize)
	}
	// TODO: check size

	return file.File.Read(int64(PageSize*page+byt), size)
}

func (file *PageFile) NewPage() (*Page, error) {
	//fmt.Printf("newPage() file:%v\n", file)
	pagenum := file.CurPage
	file.CurPage++
	data, err := file.File.Read(int64(PageSize*pagenum), PageSize)
	if err != nil {
		return nil, err
	}
	return newPage(file, pagenum, data), nil
}
