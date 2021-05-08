package page

import (
	"errors"
	"fmt"

	. "github.com/tesujiro/muzandb/errors"
)

const slottedPageHeaderBytes = 10

type SlottedPage struct {
	page         *Page
	slots        int
	freeSpacePtr int
	pctfree      float32
	data         [][]byte
}

func (sp *SlottedPage) String() string {
	var s string
	s = fmt.Sprintf("\n")
	//s = fmt.Sprintf("%vTablespace:\t%v\n", s, sp.tablespace)
	s = fmt.Sprintf("%vPage:\t%v\n", s, sp.page)
	s = fmt.Sprintf("%vSlots:\t%v\n", s, sp.slots)
	s = fmt.Sprintf("%vFreeSpacePtr:\t%v\n", s, sp.freeSpacePtr)
	s = fmt.Sprintf("%vPctfree:\t%v\n", s, sp.pctfree)
	s = fmt.Sprintf("%vData:\t%v\n", s, sp.data)
	return s
}

func NewSlottedPage(newPage NewPage) (*SlottedPage, error) {
	//fmt.Printf("ts=%v\n", ts)
	page, err := newPage()
	if err != nil {
		return nil, err
	}

	return &SlottedPage{
		//tablespace:   ts,
		page:         page,
		freeSpacePtr: PageSize,
		pctfree:      0.2,
	}, nil
}

func (sp *SlottedPage) Insert(data []byte) (*Rid, error) {
	// check size
	freeBytes := sp.freeSpacePtr - SlotBytes*sp.slots - slottedPageHeaderBytes
	if freeBytes-len(data)-SlotBytes < int(PageSize*sp.pctfree) {
		//fmt.Printf("freeBytes:%v\n", freeBytes)
		//fmt.Printf("len(data):%v\n", len(data))
		//fmt.Printf("slotBytes:%v\n", SlotBytes)
		//fmt.Printf("%v < PageSize:%v * sp.pctfree:%v\n", freeBytes-len(data)-SlotBytes, PageSize, sp.pctfree)
		return nil, NoSpaceError
	}

	// get rid
	slotnum := uint16(sp.slots)
	rid := newRid(sp.page.File, sp.page.Pagenum, slotnum)

	// set slotted page
	sp.slots++
	sp.freeSpacePtr += len(data)
	sp.data = append(sp.data, data)

	return &rid, nil
}

func (sp *SlottedPage) Select(rid *Rid) (*[]byte, error) {
	//fmt.Printf("in Select rid=%v\n", rid)
	if rid.File.FID != sp.page.File.FID {
		return nil, fmt.Errorf("rid.File.FID(%v) is not sp.File.FID(%v).", rid.File.FID, sp.page.File.FID)
	}
	if rid.Pagenum != sp.page.Pagenum {
		return nil, fmt.Errorf("rid.Pagenum(%v) is not sp.page.Pagenum(%v).", rid.Pagenum, sp.page.Pagenum)
	}
	if rid.Slotnum > uint16(sp.slots) {
		return nil, fmt.Errorf("rid.Pagenum(%v) is not sp.page.Pagenum(%v).", rid.Pagenum, sp.page.Pagenum)
	}
	return &sp.data[rid.Slotnum], nil
}

func (sp *SlottedPage) ToPageData() (*PageData, error) {
	// TODO: SAME AS ToPageDataHeader IN btree_page.go
	bytes := make([]byte, PageSize)
	index := 0

	// Header: Page Type
	bytes[index] = byte(SlottedPageType)
	index += 1
	// Header: Page Pointer
	bytes[index] = byte(sp.page.File.FID)
	index += 1
	Endian.PutUint32(bytes[index:], sp.page.Pagenum)
	index += 4
	// Header: slots
	Endian.PutUint16(bytes[index:], uint16(sp.slots))
	index += 2
	// Header: freeSpacePtr
	Endian.PutUint16(bytes[index:], uint16(sp.freeSpacePtr))
	index += 2

	index = slottedPageHeaderBytes

	dataPtr := PageSize
	for _, data := range sp.data {
		dataPtr -= len(data)
		// set slot location
		Endian.PutUint32(bytes[index:], uint32(dataPtr))
		Endian.PutUint16(bytes[index+4:], uint16(len(data)))
		index += SlotBytes
		// set data
		for j := 0; j < len(data); j++ {
			bytes[dataPtr+j] = data[j]
		}
	}

	pageData := PageData(bytes)
	return &pageData, nil
}

func (pd *PageData) ToSlottedPage(pctfree float32, getFile GetFile) (*SlottedPage, error) {

	sp := &SlottedPage{}
	bytes := []byte(*pd)
	index := 0

	pageType := PageType(bytes[index])
	index += 1
	if pageType != SlottedPageType {
		return nil, errors.New("Not a SlottedPage data")
	}

	getPage := func(data []byte, i int) (*Page, error) {
		if data[i] == 0xFF {
			return nil, nil
		}
		page := &Page{}
		fid := FID(data[i])
		file, err := getFile(fid)
		if err != nil {
			return nil, errors.New("No FID in Tablespace")
		}
		page.File = file
		i += 1
		page.Pagenum = Endian.Uint32(data[i:])

		return page, nil
	}

	// Header: Page Pointer
	page, err := getPage(bytes, index)
	if err != nil {
		return nil, err
	}
	index += 5
	sp.page = page
	// Header: slots
	sp.slots = int(Endian.Uint16(bytes[index:]))
	index += 2
	// Header: freeSpacePtr
	sp.freeSpacePtr = int(Endian.Uint16(bytes[index:]))
	index += 2
	// Header: pctfree
	sp.pctfree = pctfree

	index = slottedPageHeaderBytes

	// read slots
	slots := make([]struct {
		location uint32
		length   uint16
	}, sp.slots)
	for i := 0; i < sp.slots; i++ {
		slots[i].location = Endian.Uint32(bytes[index:])
		slots[i].length = Endian.Uint16(bytes[index+4:])
		index += SlotBytes
	}

	// read data
	sp.data = make([][]byte, sp.slots)
	for i := range sp.data {
		sp.data[i] = make([]byte, slots[i].length)
		copy(sp.data[i], bytes[slots[i].location:int(slots[i].location)+int(slots[i].length)])
		index += int(slots[i].length)
	}

	return sp, nil
}
