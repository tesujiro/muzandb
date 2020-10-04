package storage

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBtreePage(t *testing.T) {
	pm := startPageManager()

	indexfile1 := pm.NewFile("./data/indexfile1_TestBtreePage.dbf", 1024*1024)
	indexfile2 := pm.NewFile("./data/indexfile2_TestBtreePage.dbf", 1024*1024)
	datafile1 := pm.NewFile("./data/datafile1_TestBtreePage.dbf", 1024*1024)
	datafile2 := pm.NewFile("./data/datafile2_TestBtreePage.dbf", 1024*1024)

	ts_idx, err := pm.NewTablespace("INDEXSPACE1")
	if err != nil {
		t.Fatalf("PageManger.newTablespace() error:%v", err)
	}

	ts_dat, err := pm.NewTablespace("DATASPACE1")
	if err != nil {
		t.Fatalf("PageManger.newTablespace() error:%v", err)
	}

	err = ts_idx.addFile(indexfile1)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", indexfile1, err)
	}
	err = ts_idx.addFile(indexfile2)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", indexfile2, err)
	}
	err = ts_dat.addFile(datafile1)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", datafile1, err)
	}
	err = ts_dat.addFile(datafile2)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", datafile2, err)
	}

	/*
		err = pm.Save()
		if err != nil {
			t.Errorf("PageManager.Save error:%v", err)
		}
	*/

	tests := []struct {
		order    dataOrder
		elements int
		keylen   uint8
		valuelen uint8
	}{
		{order: ascendOrder, elements: 50, keylen: 200, valuelen: 200},
		{order: descendOrder, elements: 50, keylen: 200, valuelen: 200},
		{order: randomOrder, elements: 50, keylen: 200, valuelen: 200},
		{order: ascendOrder, elements: 50, keylen: 16, valuelen: 16},
		{order: descendOrder, elements: 50, keylen: 16, valuelen: 16},
		{order: randomOrder, elements: 50, keylen: 16, valuelen: 16},
	}
	//fmt.Printf("ts_idx=%v\n", ts_idx)
	//fmt.Printf("ts_dat=%v\n", ts_dat)
	fmt.Printf("datafile1=%v\n", datafile1)

	for testNumber, test := range tests {
		fmt.Printf("Testcase[%v]: %v\n", testNumber, test)
		btree, err := NewBtree(ts_idx, test.keylen, test.valuelen)
		if err != nil {
			t.Errorf("Testcase[%v]: NewBtree error:%v", testNumber, err)
		}

		keys := make([][]byte, test.elements)
		rids := make([]rid, test.elements)
		for i := range keys {
			key := make([]byte, test.keylen)
			switch test.order {
			case ascendOrder:
				copy(key, fmt.Sprintf("key%5.5v", i))
				rids[i] = newRid(datafile1, uint32(i), uint16(i))
			case descendOrder:
				copy(key, fmt.Sprintf("key%5.5v", len(keys)-1-i))
				rids[i] = newRid(datafile1, uint32(len(keys)-1-i), uint16(len(keys)-1-i))
			case randomOrder:
				copy(key, fmt.Sprintf("key%5.5v", i))
				rids[i] = newRid(datafile1, uint32(i), uint16(i))
			}
			keys[i] = key
		}
		if test.order == randomOrder {
			rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		}

		//TODO:
		for i, key := range keys {
			//rid := newRid(datafile1, uint32(i), uint16(i))
			err = btree.Insert(key, rids[i])
			//fmt.Printf("Insert key = %v\n", key)

			if err != nil {
				t.Errorf("Testcase[%v]: Insert error:%v at %s (cycle:%v)", testNumber, err, key, i)
			}
		}
		if !btree.checkLeafKeyOrder() {
			t.Errorf("Testcase[%v]: Leaf keys are not in ascend order.", testNumber)
		}

		key := make([]byte, test.keylen)
		copy(key, fmt.Sprintf("key%5.5v", rand.Intn(test.elements)))
		b, r := btree.Find(key)
		if !b {
			t.Errorf("Testcase[%v]: No key: %s in B-tree.", testNumber, key)
		}
		_ = r
		//fmt.Printf("Find(%s):%v %v\n", key, b, r)

		for _, original := range btree.walk() {
			//data := btree.ToPageDataHeader(btree.root)
			//fmt.Printf("HEADER: %v\n", data)
			data, err := btree.ToPageData(original)
			if err != nil {
				t.Errorf("Testcase[%v]: ToPageData err: %v", testNumber, err)
			}
			//fmt.Printf("PageData: %v\n", data)

			restored, err := btree.ToNode(data)
			if err != nil {
				t.Errorf("Testcase[%v]: ToNode err: %v", testNumber, err)
			}
			restored.Updated = original.Updated
			if restored.String() != original.String() {
				t.Errorf("Testcase[%v]: Restored node != Original node", testNumber)
				t.Errorf("Original Node: %v\n", original)
				t.Errorf("Restored Node: %v\n", restored)
				t.Errorf("data: %v\n", data)
				//t.Errorf("data:\n%s", hex.Dump([]byte(*data)))
			}
		}
	}
}
