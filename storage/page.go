package storage

import (
	"encoding/binary"
)

const BlockSize = 1024

var endian binary.ByteOrder = binary.BigEndian

type page struct {
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

func newPage() *page {
	data := make([]byte, BlockSize)
	h := pageHeader{}
	p := &page{data: data, header: h}
	return p
}

func getPage(bl []byte) *page {
	p := &page{data: bl}
	p.header = p.readHeader()
	return p
}

func (p *page) readHeader() pageHeader {
	header := p.data[len(p.data)-pageHeaderBytes:]
	slots := endian.Uint16(header[:2])
	fsp := endian.Uint16(header[2:])
	return pageHeader{slots: slots, freeSpacePointer: fsp}
}

func (p *page) setHeader(ph pageHeader) {
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

func (p *page) insertRecord(rec record) (slot, error) {
	//TODO: check

	var lastSlot slot
	if p.header.slots > 0 {
		lastSlot = p.getSlot(p.header.slots)
	}

	header := pageHeader{slots: p.header.slots + 1, freeSpacePointer: p.header.freeSpacePointer + uint16(len(rec))}
	p.setHeader(header)

	location := lastSlot.location + lastSlot.length
	sl := slot{location: location, length: uint16(len(rec))}
	p.setSlot(header.slots, sl)
	for i, c := range rec {
		p.data[int(location)+i] = c
	}

	return sl, nil
}

func (p *page) getSlot(slotnum uint16) slot {
	slotlocation := uint16(len(p.data)) - pageHeaderBytes - slotBytes*slotnum
	slotb := p.data[slotlocation : slotlocation+slotBytes]
	loc := endian.Uint16(slotb)
	leng := endian.Uint16(slotb[2:])
	return slot{location: loc, length: leng}
}

func (p *page) setSlot(slotnum uint16, sl slot) {
	b := make([]byte, 4)
	endian.PutUint16(b[0:], sl.location)
	endian.PutUint16(b[2:], sl.length)

	loc := uint16(len(p.data)) - pageHeaderBytes - slotnum*slotBytes
	for i, r := range b {
		p.data[int(loc)+i] = r
	}

	return
}

func (p *page) getRecord(sl slot) record {
	return p.data[sl.location : sl.location+sl.length]
}

func (p *page) selectRecord(slotnum uint16) record {
	sl := p.getSlot(slotnum)
	return p.getRecord(sl)
}
