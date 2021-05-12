package page

import (
	"encoding/binary"
	"fmt"
	//. "github.com/tesujiro/muzandb/errors"
)

var Endian binary.ByteOrder = binary.BigEndian

//const PageSize = 512
const PageSize = 1024

type PageType uint8

const (
	BtreeLeafPageType PageType = iota
	BtreeNonLeafPageType
	SlottedPageType
)

type PageData []byte

func (pd PageData) String() string {
	return fmt.Sprintf("%x", []byte(pd))
}

type Page struct {
	File    *PageFile
	Pagenum uint32

	//FixedSizedRecord bool
	data   []byte
	header pageHeader
}

func newPage(file *PageFile, pagenum uint32, data []byte) *Page {
	p := &Page{File: file, Pagenum: pagenum, data: data}
	//p.header = p.readHeader()
	return p
}

func (p *Page) write() error {
	//return p.File.Write(p.Pagenum, 0, p.data)
	//return p.File.Write()
	return p.File.Write(int64(p.Pagenum*PageSize), p.data)
}

const PagePointerBytes = 5

type FID uint8

type NewPage func() (*Page, error)
type GetFile func(FID) (*PageFile, error)

func (p *Page) String() string {
	return fmt.Sprintf("Page:(file.path=%v, pagenum=%v)", p.File.Path, p.Pagenum)
}

type pageHeader struct {
	slots            uint16
	freeSpacePointer uint16
}

const PageHeaderBytes = 20
