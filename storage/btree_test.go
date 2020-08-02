package storage

import (
	"fmt"
	"testing"
)

func TestBtree(t *testing.T) {

	pm := startPageManager()

	indexfile1 := pm.NewFile("./data/indexfile1.dbf", 1024*1024)
	indexfile2 := pm.NewFile("./data/indexfile2.dbf", 1024*1024)
	datafile1 := pm.NewFile("./data/datafile1.dbf", 1024*1024)
	datafile2 := pm.NewFile("./data/datafile2.dbf", 1024*1024)

	ts1, err := pm.NewTablespace("INDEXSPACE1")
	if err != nil {
		t.Fatalf("PageManger.newTablespace() error:%v", err)
	}
	ts2, err := pm.NewTablespace("DATASPACE1")
	if err != nil {
		t.Fatalf("PageManger.newTablespace() error:%v", err)
	}

	err = ts1.addFile(indexfile1)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", indexfile1, err)
	}
	err = ts1.addFile(indexfile2)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", indexfile2, err)
	}
	err = ts2.addFile(datafile1)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", datafile1, err)
	}
	err = ts2.addFile(datafile2)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", datafile2, err)
	}
	fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

	/*
		err = pm.Save()
		if err != nil {
			t.Errorf("PageManager.Save error:%v", err)
		}
	*/

	btree, err := NewBtree(ts1, 16, 16)
	if err != nil {
		t.Errorf("NewBtree error:%v", err)
	}

	for i := 0; i < 69; i++ {
		rid := newRid(datafile1, 0, uint16(i))
		key := fmt.Sprintf("key%3.3d", i)
		err = btree.Insert([]byte(key), rid)
		if err != nil {
			t.Errorf("Insert error:%v", err)
		}
	}

	btree.PrintLeaves()

}
