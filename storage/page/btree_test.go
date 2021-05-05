package page

import (
	"fmt"
	"math/rand"
	"testing"
)

type dataOrder uint8

const (
	ascendOrder dataOrder = iota
	descendOrder
	randomOrder
)

func TestBtree(t *testing.T) {

	//debug.On()

	//pm := startPageManager()

	const (
		indexFID = FID(1)
		dataFID  = FID(2)
	)

	indexfile1 := NewPageFile(indexFID, "./data/TestBtree_indexfile1.dbf", 1024*1024)
	datafile1 := NewPageFile(dataFID, "./data/TestBtree_datafile1.dbf", 1024*1024)

	getFile := func(fid FID) (*PageFile, error) {
		switch fid {
		case 1:
			return indexfile1, nil
		default:
			return datafile1, nil
		}
	}

	//indexfile1 := pm.NewFile("./data/TestBtree_indexfile1.dbf", 1024*1024)
	//indexfile2 := pm.NewFile("./data/TestBtree_indexfile2.dbf", 1024*1024)
	//datafile1 := pm.NewFile("./data/TestBtree_datafile1.dbf", 1024*1024)
	//datafile2 := pm.NewFile("./data/TestBtree_datafile2.dbf", 1024*1024)

	/*
		ts1, err := pm.NewTablespace("INDEXSPACE1")
		if err != nil {
			t.Fatalf("PageManger.newTablespace() error:%v", err)
		}

		err = ts1.addFile(indexfile1)
		if err != nil {
			t.Errorf("Tablespace.addFile(%v) error:%v", indexfile1, err)
		}
	*/
	/*
		err = ts1.addFile(indexfile2)
		if err != nil {
			t.Errorf("Tablespace.addFile(%v) error:%v", indexfile2, err)
		}
	*/

	/*
		ts2, err := pm.NewTablespace("DATASPACE1")
		if err != nil {
			t.Fatalf("PageManger.newTablespace() error:%v", err)
		}

		err = ts2.addFile(datafile1)
		if err != nil {
			t.Errorf("Tablespace.addFile(%v) error:%v", datafile1, err)
		}
		err = ts2.addFile(datafile2)
		if err != nil {
			t.Errorf("Tablespace.addFile(%v) error:%v", datafile2, err)
		}
	*/
	//fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

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
		{order: ascendOrder, elements: 10000, keylen: 16, valuelen: 16},
		{order: descendOrder, elements: 10000, keylen: 16, valuelen: 16},
		{order: randomOrder, elements: 10000, keylen: 16, valuelen: 16},
	}

	for testNumber, test := range tests {
		fmt.Printf("Testcase[%v]: %v\n", testNumber, test)
		//btree, err := NewBtree(ts1.NewPage, pm.GetFile, test.keylen, test.valuelen)
		btree, err := NewBtree(indexfile1.NewPage, getFile, test.keylen, test.valuelen)
		if err != nil {
			t.Errorf("Testcase[%v]: NewBtree error:%v", testNumber, err)
		}

		keys := make([][]byte, test.elements)
		for i := range keys {
			key := make([]byte, test.keylen)
			switch test.order {
			case ascendOrder:
				copy(key, fmt.Sprintf("key%5.5v", i))
			case descendOrder:
				copy(key, fmt.Sprintf("key%5.5v", len(keys)-1-i))
			case randomOrder:
				copy(key, fmt.Sprintf("key%5.5v", i))
			}
			keys[i] = key
		}
		if test.order == randomOrder {
			rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		}

		rid := newRid(datafile1, 0, 0)
		for i, key := range keys {
			err = btree.Insert([]byte(key), rid)
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
	}
}
