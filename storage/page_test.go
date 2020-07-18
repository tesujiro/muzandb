package storage

import (
	"fmt"
	"testing"
)

func TestPage(t *testing.T) {
	f, err := newFile("yyy.dbf", BlockSize*10)
	if err != nil {
		fmt.Println(err)
	}
	defer f.close()

	// Test Page Header
	b := make([]byte, 4)
	endian.PutUint16(b[0:], uint16(10000))
	endian.PutUint16(b[2:], uint16(20000))
	err = f.write(1, BlockSize-4, b)
	if err != nil {
		fmt.Println(err)
	}

	buf, err := f.readBlock(1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("readBlock()=%v\n", buf)
	}
	page := getPage(buf)
	fmt.Printf("header[%v] slots=%v freeSpacePointer=%v\n", page.header, page.header.slots, page.header.freeSpacePointer)

	// Test Page Record
	endian.PutUint16(b[0:], uint16(10))
	endian.PutUint16(b[2:], uint16(20))
	err = f.write(1, BlockSize-4-4, b)
	if err != nil {
		fmt.Println(err)
	}
	err = f.write(1, 10, []byte("....5....0....5....0"))

	buf, err = f.readBlock(1)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("readBlock()=%v\n", buf)
	}

	page = getPage(buf)
	r := page.selectRecord(1)
	fmt.Printf("record(%v)=%v\n", len(r), string(r))

}
