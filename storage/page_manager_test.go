package storage

import (
	"bytes"
	"fmt"
	"testing"
)

func TestPageManager(t *testing.T) {
	var err error

	pm := startPageManager()

	file1 := pm.NewFile("./data/file1.dbf", 1024*1024)
	file2 := pm.NewFile("./data/file2.dbf", 1024*1024)
	file3 := pm.NewFile("./data/file3.dbf", 1024*1024)

	ts1, err := pm.NewTablespace("TABLESPACE1")
	if err != nil {
		t.Fatalf("PageManger.newTablespace() error:%v", err)
	}

	err = ts1.addFile(file1)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", file1, err)
	}
	err = ts1.addFile(file2)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", file2, err)
	}
	err = ts1.addFile(file3)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", file3, err)
	}
	fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

	err = pm.Save()
	if err != nil {
		t.Errorf("PageManager.Save error:%v", err)
	}

	err = pm.Stop()
	if err != nil {
		t.Errorf("PageManager.Stop error:%v", err)
	}
	pm = startPageManager()
	if len(pm.Tablespaces) != 1 {
		t.Errorf("startPageManager result len(pm.Tablespaces) want: %v actual: %v", 1, len(pm.Tablespaces))

	}
	if pm.Tablespaces[0].Name != "TABLESPACE1" {
		t.Errorf("startPageManager result pm.Tablespaces[0].Name want: %v actual: %v", "TABLESPACE1", pm.Tablespaces[0].Name)

	}
	if len(pm.Tablespaces[0].File) != 3 {
		t.Errorf("startPageManager result len(pm.Tablespaces[0].File) want: %v actual: %v", 3, len(pm.Tablespaces[0].File))

	}
	fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

	ts0 := pm.Tablespaces[0]
	page, err := ts0.NewPage()
	if err != nil {
		t.Errorf("file.readPage error:%v", err)
		//} else {
		//fmt.Printf("readPage()=%v\n", buf)
	}
	fmt.Printf("pagenum=%v\n", page.pagenum)
	pagenum := page.pagenum
	fmt.Printf("header[%v] slots=%v freeSpacePointer=%v\n", page.header, page.header.slots, page.header.freeSpacePointer)

	rec0 := []byte("THIS IS A TUPLE 1")
	slot0, err := page.InsertRecord(rec0)
	if err != nil {
		t.Errorf("page.InsertRecord error:%v", err)
	} else if slot0.slotnum != uint16(0) {
		t.Errorf("page.InsertRecord result slot.slotnum want: %v actual: %v", uint16(0), slot0.slotnum)
	} else if slot0.location != uint16(0) {
		t.Errorf("page.InsertRecord result slot.location want: %v actual: %v", uint16(0), slot0.location)
	} else if slot0.length != uint16(len(rec0)) {
		t.Errorf("page.InsertRecord result slot.length want: %v actual: %v", uint16(len(rec0)), slot0.length)
	}
	rec1 := []byte("THIS IS A TUPLE 2")
	slot1, err := page.InsertRecord(rec1)
	if err != nil {
		t.Errorf("page.InsertRecord error:%v", err)
	} else if slot1.slotnum != uint16(1) {
		t.Errorf("page.InsertRecord result slot.slotnum want: %v actual: %v", uint16(1), slot1.slotnum)
	} else if slot1.location != slot0.length {
		t.Errorf("page.InsertRecord result slot.location want: %v actual: %v", slot0.length, slot1.location)
	} else if slot1.length != uint16(len(rec1)) {
		t.Errorf("page.InsertRecord result slot.length want: %v actual: %v", uint16(len(rec1)), slot1.length)
	}
	rec2 := []byte("THIS IS A TUPLE 2")
	slot2, err := page.InsertRecord(rec2)
	if err != nil {
		t.Errorf("page.InsertRecord error:%v", err)
	} else if slot2.slotnum != uint16(2) {
		t.Errorf("page.InsertRecord result slot.slotnum want: %v actual: %v", uint16(2), slot2.slotnum)
	} else if slot2.location != slot0.length+slot1.length {
		t.Errorf("page.InsertRecord result slot.location want: %v actual: %v", slot0.length+slot1.length, slot2.location)
	} else if slot2.length != uint16(len(rec2)) {
		t.Errorf("page.InsertRecord result slot.length want: %v actual: %v", uint16(len(rec2)), slot2.length)
	}
	err = page.DeleteRecord(1)
	if err != nil {
		t.Errorf("page.DeleteRecord error:%v", err)
	}
	err = page.DeleteRecord(1)
	if err != AlreadyDeletedError {
		t.Errorf("page.DeleteRecord != AlreadyDeletedError error:%v", err)
	}
	err = page.UpdateRecord(1, rec1)
	if err != AlreadyDeletedError {
		t.Errorf("page.UpdateRecord != AlreadyDeletedError error:%v", err)
	}
	rec2 = []byte("THIS IS A NEW TUPLE 3")
	err = page.UpdateRecord(2, rec2)
	if err != nil {
		t.Errorf("page.UpdateRecord error:%v", err)
	}
	rec2 = []byte("THIS IS A NEW TUPLE 3")
	err = page.UpdateRecord(2, rec2)
	if err != nil {
		t.Errorf("page.UpdateRecord error:%v", err)
	}

	err = page.write()
	if err != nil {
		t.Errorf("file.writePage error:%v", err)
	}

	// read the same page again
	file0 := page.file
	page, err = file0.readPage(pagenum)
	if err != nil {
		t.Errorf("file.readPage error:%v", err)
	} else {
		fmt.Printf("readPage()=%v\n", page)
	}
	//TODO: page.SelectRecord(4)
	//TODO: page.SelectRecord(1)
	rec, err := page.SelectRecord(2)
	if err != nil {
		t.Errorf("page.SelectRecord error:%v", err)
	} else if !bytes.Equal(rec, rec2) {
		t.Errorf("page.SelectRecord result record want: %v actual: %v", rec2, rec)
	}
}
