package storage

import (
	"encoding/binary"
	"errors"
)

const BlockSize = 1024

var endian binary.ByteOrder = binary.BigEndian

var (
	AlreadyDeletedError = errors.New("Already deleted")
	NoSpaceError        = errors.New("The page does not have enough space")
	NoSuchSlotError     = errors.New("The page does not have the slot")
)

type Page struct {
	data   []byte
	header pageHeader
}

type pageHeader struct {
	slots            uint16
	freeSpacePointer uint16
}

const pageHeaderBytes = 4

type record []byte

type slot struct {
	location uint16
	length   uint16
}

const slotBytes = 4

type rid struct {
	pageid  uint32
	slotnum uint16
}

/*
func NewPage() *Page {
	data := make([]byte, BlockSize)
	h := pageHeader{}
	p := &Page{data: data, header: h}
	return p
}
*/

func NewPage(bl []byte) *Page {
	p := &Page{data: bl}
	p.header = p.readHeader()
	return p
}

func (p *Page) readHeader() pageHeader {
	header := p.data[len(p.data)-pageHeaderBytes:]
	slots := endian.Uint16(header[:2])
	fsp := endian.Uint16(header[2:])
	return pageHeader{slots: slots, freeSpacePointer: fsp}
}

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
	newSlots := p.header.slots + 1
	newFSPointer := p.header.freeSpacePointer + uint16(len(rec))

	if newFSPointer >= uint16(len(p.data))-pageHeaderBytes-slotBytes*newSlots {
		return nil, NoSpaceError
	}

	header := pageHeader{slots: newSlots, freeSpacePointer: newFSPointer}
	p.setHeader(header)

	sl := &slot{location: location, length: uint16(len(rec))}
	p.setSlot(header.slots, sl)

	// set Record
	for i, c := range rec {
		p.data[int(location)+i] = c
	}

	return sl, nil
}

func (p *Page) getSlot(slotnum uint16) (*slot, error) {
	if slotnum > p.header.slots {
		return nil, NoSuchSlotError
	}
	slotlocation := uint16(len(p.data)) - pageHeaderBytes - slotBytes*slotnum
	slotb := p.data[slotlocation : slotlocation+slotBytes]
	loc := endian.Uint16(slotb)
	leng := endian.Uint16(slotb[2:])
	return &slot{location: loc, length: leng}, nil
}

func (p *Page) setSlot(slotnum uint16, sl *slot) error {
	// TODO: check No Space

	b := make([]byte, 4)
	endian.PutUint16(b[0:], sl.location)
	endian.PutUint16(b[2:], sl.length)

	loc := uint16(len(p.data)) - pageHeaderBytes - slotnum*slotBytes
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

	sl = &slot{location: loc, length: uint16(len(rec))}
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
