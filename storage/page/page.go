package page

import (
	"encoding/binary"
	"fmt"

	. "github.com/tesujiro/muzandb/errors"
	"github.com/tesujiro/muzandb/storage/fio"
	//"github.com/tesujiro/muzandb/storage/fio"
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

const PagePointerBytes = 5

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

type FID uint8

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

type record []byte

type slot struct {
	slotnum  uint16
	location uint16
	length   uint16
}

const SlotBytes = 6

type Rid struct {
	File    *PageFile
	Pagenum uint32
	Slotnum uint16
}

func (r Rid) String() string {
	return fmt.Sprintf("File:%s Pagenum:%d Slotnum:%d", r.File.Path, r.Pagenum, r.Slotnum)
}

const RidBytes = 7

func newRid(file *PageFile, pagenum uint32, slotnum uint16) Rid {
	return Rid{
		File:    file,
		Pagenum: pagenum,
		Slotnum: slotnum,
	}
}

func (rid Rid) Bytes() []byte {
	var rid_b, pagenum_b, slotnum_b []byte
	rid_b = []byte{byte(rid.File.FID)}
	Endian.PutUint32(pagenum_b, rid.Pagenum)
	Endian.PutUint16(slotnum_b, rid.Slotnum)
	return append(append(rid_b, pagenum_b...), slotnum_b...)
}

func newPage(file *PageFile, pagenum uint32, bl []byte) *Page {
	p := &Page{File: file, Pagenum: pagenum}
	//p.header = p.readHeader()
	return p
}

//TODO
func (p *Page) write() error {
	//return p.file.write(p.Pagenum, 0, p.data)
	return nil
}

func (p *Page) setHeader(ph pageHeader) {
	p.header.slots = ph.slots
	p.header.freeSpacePointer = ph.freeSpacePointer

	b := make([]byte, PageHeaderBytes)
	Endian.PutUint16(b[0:], ph.slots)
	Endian.PutUint16(b[2:], ph.freeSpacePointer)

	loc := len(p.data) - PageHeaderBytes
	for i, r := range b {
		p.data[int(loc)+i] = r
	}
	return
}

func (p *Page) InsertRecord(rec record) (*slot, error) {
	location := p.header.freeSpacePointer
	slotnum := p.header.slots
	newSlots := p.header.slots + 1
	newFSPointer := p.header.freeSpacePointer + uint16(len(rec))

	if newFSPointer >= uint16(len(p.data))-PageHeaderBytes-SlotBytes*newSlots {
		return nil, NoSpaceError
	}

	header := pageHeader{slots: newSlots, freeSpacePointer: newFSPointer}
	p.setHeader(header)

	sl := &slot{slotnum: slotnum, location: location, length: uint16(len(rec))}
	p.setSlot(header.slots, sl)

	// set Record
	for i, c := range rec {
		p.data[int(location)+i] = c
	}

	return sl, nil
}

func (page *Page) InsertRecordAt(rec record, index uint16) error {
	if page.header.slots < index {
		return fmt.Errorf("insertAt error: index larger than slots")
	}
	if page.header.slots == 0 {
		_, err := page.InsertRecord(rec)
		return err
	}
	for i := page.header.slots; i > uint16(index); i-- {
		record, err := page.SelectRecord(i - 1)
		if err != nil {
			return err
		}
		if i == page.header.slots {
			page.InsertRecord(record)
		} else {
			page.UpdateRecord(i, record)
		}
	}
	page.UpdateRecord(uint16(index), rec)

	return nil
}

func (p *Page) getSlot(slotnum uint16) (*slot, error) {
	if slotnum > p.header.slots-1 {
		return nil, NoSuchSlotError
	}
	slotlocation := uint16(len(p.data)) - PageHeaderBytes - SlotBytes*(slotnum+1)
	slotb := p.data[slotlocation : slotlocation+SlotBytes]
	loc := Endian.Uint16(slotb)
	leng := Endian.Uint16(slotb[2:])
	return &slot{slotnum: slotnum, location: loc, length: leng}, nil
}

func (p *Page) setSlot(slotnum uint16, sl *slot) error {
	// TODO: check No Space

	b := make([]byte, 4)
	Endian.PutUint16(b[0:], sl.location)
	Endian.PutUint16(b[2:], sl.length)

	loc := uint16(len(p.data)) - PageHeaderBytes - SlotBytes*(slotnum+1)
	for i, r := range b {
		p.data[int(loc)+i] = r
	}

	return nil
}

func (p *Page) deleteSlot(slotnum uint16) error {
	sl, err := p.getSlot(slotnum)
	if err != nil {
		return err
	}
	if sl.deleted() {
		return AlreadyDeletedError
	}
	sl.setDeleted()
	return p.setSlot(slotnum, sl)
}

func (p *Page) getRecord(sl slot) (record, error) {
	if sl.deleted() {
		return nil, AlreadyDeletedError
	}
	return p.data[sl.location : sl.location+sl.length], nil
}

func (p *Page) SelectRecord(slotnum uint16) (record, error) {
	sl, err := p.getSlot(slotnum)
	if err != nil {
		return nil, err
	}
	return p.getRecord(*sl)
}

func (p *Page) UpdateRecord(slotnum uint16, rec record) error {
	sl, err := p.getSlot(slotnum)
	if err != nil {
		return err
	}
	if sl.deleted() {
		return AlreadyDeletedError
	}

	var loc uint16
	if sl.length >= uint16(len(rec)) {
		loc = sl.location
	} else {
		// new location
		loc = p.header.freeSpacePointer
		slots := p.header.slots
		newFSPointer := p.header.freeSpacePointer + uint16(len(rec))

		if newFSPointer >= uint16(len(p.data))-PageHeaderBytes-SlotBytes*slots {
			return NoSpaceError
		}
	}

	p.header.freeSpacePointer += uint16(len(rec))
	p.setHeader(p.header)

	sl = &slot{slotnum: slotnum, location: loc, length: uint16(len(rec))}
	p.setSlot(slotnum, sl)

	// set Record
	for i, c := range rec {
		p.data[int(loc)+i] = c
	}

	return nil
}

func (p *Page) DeleteRecord(slotnum uint16) error {
	sl, err := p.getSlot(slotnum)
	if err != nil {
		return err
	}
	if sl.deleted() {
		return AlreadyDeletedError
	}
	sl.setDeleted()
	p.setSlot(slotnum, sl)
	return nil
}

func (sl *slot) setDeleted() {
	sl.location += 1 << 15
}

func (sl *slot) deleted() bool {
	return sl.location>>15 == 1
}
