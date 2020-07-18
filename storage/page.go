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
	header := bl[len(bl)-pageHeaderBytes:]
	//fmt.Printf("header[%v]\n", header)
	//var slots, fsp uint16
	slots := endian.Uint16(header[:2])
	fsp := endian.Uint16(header[2:])
	//fmt.Printf("header[%v] slots=%v freeSpacePointer=%v\n", header, slots, fsp)
	h := pageHeader{slots: slots, freeSpacePointer: fsp}
	p := &page{data: bl, header: h}
	return p
}

func (p *page) setHeader(ph pageHeader) error {
	return nil
}

func (p *page) insertRecord(r record) error {
	return nil
}

func (p *page) getSlot(slotnum uint16) slot {
	slotlocation := BlockSize - pageHeaderBytes - slotBytes*slotnum
	slotb := p.data[slotlocation : slotlocation+slotBytes]
	loc := endian.Uint16(slotb)
	leng := endian.Uint16(slotb[2:])
	return slot{location: loc, length: leng}
}

func (p *page) getRecord(sl slot) record {
	return p.data[sl.location : sl.location+sl.length]
}

func (p *page) selectRecord(slotnum uint16) record {
	sl := p.getSlot(slotnum)
	return p.getRecord(sl)
}
