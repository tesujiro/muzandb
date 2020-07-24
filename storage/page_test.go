package storage

import (
	"fmt"
	"testing"
)

func TestPage(t *testing.T) {
	f := newFile(1, "yyy.dbf", PageSize*10)
	f.create()
	defer f.close()

	// Test Page Header
	b := make([]byte, 4)
	endian.PutUint16(b[0:], uint16(10000))
	endian.PutUint16(b[2:], uint16(20000))
	err := f.write(1, PageSize-4, b)
	if err != nil {
		fmt.Println(err)
	}

	buf, err := f.readPage(1)
	if err != nil {
		fmt.Println(err)
	} else {
		//fmt.Printf("readPage()=%v\n", buf)
	}
	page := NewPage(buf)
	fmt.Printf("header[%v] slots=%v freeSpacePointer=%v\n", page.header, page.header.slots, page.header.freeSpacePointer)

	// Test Page Record
	s := make([]byte, 4)
	endian.PutUint16(s[0:], uint16(10))
	endian.PutUint16(s[2:], uint16(20))
	for i, c := range s {
		page.data[PageSize-4-4+i] = c
	}
	//fmt.Printf("page.data=%v\n", page.data)
	err = f.writePage(1, page.data)
	if err != nil {
		fmt.Println(err)
	}
	err = f.write(1, 10, []byte("....5....0....5....0"))

	buf, err = f.readPage(1)
	if err != nil {
		fmt.Println(err)
	} else {
		//fmt.Printf("readPage()=%v\n", buf)
	}

	page = NewPage(buf)
	r, _ := page.SelectRecord(1)
	fmt.Printf("record(%v)=%v\n", len(r), string(r))

	// Test Insert Record
	pagenum := uint32(2)
	buf, err = f.readPage(pagenum)
	if err != nil {
		fmt.Println(err)
	} else {
		//fmt.Printf("readPage()=%v\n", buf)
	}

	page = NewPage(buf)
	_, err = page.InsertRecord([]byte("TEST RECORD1"))
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("slot=%v\n", sl)
	_, err = page.InsertRecord([]byte("TEST RECORD2"))
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("slot=%v\n", sl)
	_, err = page.InsertRecord([]byte("TEST RECORD3"))
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Printf("slot=%v\n", sl)
	err = page.DeleteRecord(2)
	if err != nil {
		fmt.Println(err)
	}
	err = page.DeleteRecord(2)
	if err != nil {
		fmt.Println(err)
	}
	err = page.UpdateRecord(1, []byte("TEST RECORD1 NEW"))
	if err != nil {
		fmt.Println(err)
	}

	err = f.writePage(pagenum, page.data)
	if err != nil {
		fmt.Println(err)
	}
	buf, err = f.readPage(pagenum)
	if err != nil {
		fmt.Println(err)
	} else {
		//fmt.Printf("readPage()=%v\n", buf)
	}

}
