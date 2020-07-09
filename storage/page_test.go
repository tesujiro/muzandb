package storage

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"testing"
	"unsafe"
)

func TestPage(t *testing.T) {
	header := PageHeader{Pd_checksum: 100}
	page := Page{PageHeader: &header}

	fmt.Printf("page=%v\n", page)
	fmt.Printf("sizeof(page)=%v\n", unsafe.Sizeof(page))
	fmt.Printf("sizeof(page.PageHeader)=%v\n", unsafe.Sizeof(page.PageHeader))
	fmt.Printf("sizeof(page.PageHeader.Pd_lsn)=%v\n", unsafe.Sizeof(page.PageHeader.Pd_lsn))
	fmt.Printf("sizeof(page.PageHeader.Pd_checksum)=%v\n", unsafe.Sizeof(page.PageHeader.Pd_checksum))

	buff := new(bytes.Buffer)
	enc := gob.NewEncoder(buff)
	if err := enc.Encode(page.PageHeader); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("buff=%v\n", buff.Bytes())
	fmt.Printf("buff=%s\n", buff.Bytes())
	fmt.Printf("len(buff)=%v\n", len(buff.Bytes()))

}
