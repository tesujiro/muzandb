package storage

import (
	"fmt"
	"testing"
)

func TestPageManager(t *testing.T) {
	var err error

	pm := startPageManager()

	file1 := pm.newFile("./data/file1.dbf", 1024*1024)
	file2 := pm.newFile("./data/file2.dbf", 1024*1024)
	file3 := pm.newFile("./data/file3.dbf", 1024*1024)

	ts1, err := pm.newTablespace("TABLESPACE1")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	err = ts1.addFile(file1)
	if err != nil {
		fmt.Printf("addFile(%v) error: %v\n", file1, err)
	}
	err = ts1.addFile(file2)
	if err != nil {
		fmt.Printf("addFile(%v) error: %v\n", file2, err)
	}
	err = ts1.addFile(file3)
	if err != nil {
		fmt.Printf("addFile(%v) error: %v\n", file3, err)
	}
	fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

	err = pm.Save()
	if err != nil {
		fmt.Printf("save error: %v\n", err)
	}

	err = pm.Stop()
	if err != nil {
		fmt.Printf("stop error: %v\n", err)
	}
	pm = startPageManager()
	fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

	file0 := pm.Tablespaces[0].File[0]
	pagenum := uint32(0)
	buf, err := file0.readPage(pagenum)
	if err != nil {
		t.Errorf("file.readPage error:%v", err)
		//} else {
		//fmt.Printf("readPage()=%v\n", buf)
	}
	page := NewPage(buf)
	fmt.Printf("header[%v] slots=%v freeSpacePointer=%v\n", page.header, page.header.slots, page.header.freeSpacePointer)

	rec1 := []byte("THIS IS A TUPLE 1")
	slot1, err := page.InsertRecord(rec1)
	if err != nil {
		t.Errorf("page.InsertRecord error:%v", err)
	} else if slot1.location != uint16(0) {
		t.Errorf("page.InsertRecord result slot.location want: %v actual: %v", uint16(0), slot1.location)
	} else if slot1.length != uint16(len(rec1)) {
		t.Errorf("page.InsertRecord result slot.length want: %v actual: %v", uint16(len(rec1)), slot1.length)
	}
	rec2 := []byte("THIS IS A TUPLE 2")
	slot2, err := page.InsertRecord(rec2)
	if err != nil {
		t.Errorf("page.InsertRecord error:%v", err)
	} else if slot2.location != slot1.length {
		t.Errorf("page.InsertRecord result slot.location want: %v actual: %v", slot1.length, slot2.location)
	} else if slot2.length != uint16(len(rec2)) {
		t.Errorf("page.InsertRecord result slot.length want: %v actual: %v", uint16(len(rec2)), slot2.length)
	}
	rec3 := []byte("THIS IS A TUPLE 2")
	slot3, err := page.InsertRecord(rec3)
	if err != nil {
		t.Errorf("page.InsertRecord error:%v", err)
	} else if slot3.location != slot1.length+slot2.length {
		t.Errorf("page.InsertRecord result slot.location want: %v actual: %v", slot1.length+slot2.length, slot3.location)
	} else if slot3.length != uint16(len(rec3)) {
		t.Errorf("page.InsertRecord result slot.length want: %v actual: %v", uint16(len(rec3)), slot3.length)
	}
	err = page.DeleteRecord(2)
	if err != nil {
		t.Errorf("page.DeleteRecord error:%v", err)
	}
	err = file0.writePage(pagenum, buf)
	if err != nil {
		t.Errorf("file.writePage error:%v", err)
	}
	buf, err = file0.readPage(pagenum)
	if err != nil {
		t.Errorf("file.readPage error:%v", err)
	} else {
		fmt.Printf("readPage()=%v\n", buf)
	}
}
