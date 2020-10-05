package storage

import (
	"encoding/binary"
	"fmt"
)

var endian binary.ByteOrder = binary.BigEndian

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
	file    *File
	pagenum uint32

	//FixedSizedRecord bool
	data   []byte
	header pageHeader
}

/*
type pagePointer struct {
	file    *File
	pagenum uint32
}
*/

const pagePointerBytes = 5

func (p *Page) String() string {
	return fmt.Sprintf("Page:(file.path=%v, pagenum=%v)", p.file.Path, p.pagenum)
}

/*
func (p *Page) Bytes() []byte {
}
*/

/*
type SlottedPage struct {
	Page

	//FixedSizedRecord bool
	//data   []byte
	header pageHeader
}

func NewSlottedPage(file *File, pagenum uint32) *SlottedPage {
	return &SlottedPage{
		Page: Page{
			file:    file,
			pagenum: pagenum,
		},
	}
}

func (sp *SlottedPage) Bytes() (*[]byte, error) {
	return nil, nil
}
*/

type pageHeader struct {
	slots            uint16
	freeSpacePointer uint16
}

const pageHeaderBytes = 20

type record []byte

type slot struct {
	slotnum  uint16
	location uint16
	length   uint16
}

const slotBytes = 6

type rid struct {
	file    *File
	pagenum uint32
	slotnum uint16
}

func (r rid) String() string {
	return fmt.Sprintf("File:%s Pagenum:%d Slotnum:%d", r.file.Path, r.pagenum, r.slotnum)
}

const ridBytes = 7

func newRid(file *File, pagenum uint32, slotnum uint16) rid {
	return rid{
		file:    file,
		pagenum: pagenum,
		slotnum: slotnum,
	}
}

func (rid rid) Bytes() []byte {
	var rid_b, pagenum_b, slotnum_b []byte
	rid_b = []byte{byte(rid.file.FID)}
	endian.PutUint32(pagenum_b, rid.pagenum)
	endian.PutUint16(slotnum_b, rid.slotnum)
	return append(append(rid_b, pagenum_b...), slotnum_b...)
}

func newPage(file *File, pagenum uint32, bl []byte) *Page {
	p := &Page{file: file, pagenum: pagenum}
	//p.header = p.readHeader()
	return p
}

func (p *Page) write() error {
	//return p.file.write(p.pagenum, 0, p.data)
	return nil
}

/*
func (p *Page) readHeader() pageHeader {
	header := p.data[len(p.data)-pageHeaderBytes:]
	slots := endian.Uint16(header[:2])
	fsp := endian.Uint16(header[2:])
	return pageHeader{slots: slots, freeSpacePointer: fsp}
}
*/

func (p *Page) setHeader(ph pageHeader) {
	p.header.slots = ph.slots
	p.header.freeSpacePointer = ph.freeSpacePointer

	b := make([]byte, pageHeaderBytes)
	endian.PutUint16(b[0:], ph.slots)
	endian.PutUint16(b[2:], ph.freeSpacePointer)

	loc := len(p.data) - pageHeaderBytes
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

	if newFSPointer >= uint16(len(p.data))-pageHeaderBytes-slotBytes*newSlots {
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
	slotlocation := uint16(len(p.data)) - pageHeaderBytes - slotBytes*(slotnum+1)
	slotb := p.data[slotlocation : slotlocation+slotBytes]
	loc := endian.Uint16(slotb)
	leng := endian.Uint16(slotb[2:])
	return &slot{slotnum: slotnum, location: loc, length: leng}, nil
}

func (p *Page) setSlot(slotnum uint16, sl *slot) error {
	// TODO: check No Space

	b := make([]byte, 4)
	endian.PutUint16(b[0:], sl.location)
	endian.PutUint16(b[2:], sl.length)

	loc := uint16(len(p.data)) - pageHeaderBytes - slotBytes*(slotnum+1)
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

		if newFSPointer >= uint16(len(p.data))-pageHeaderBytes-slotBytes*slots {
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
