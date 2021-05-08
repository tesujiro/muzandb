package page

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

func TestBtreePage(t *testing.T) {
	//debug.On()
	//pm := startPageManager()

	const (
		indexFID1 = FID(1)
		indexFID2 = FID(2)
		dataFID1  = FID(3)
		dataFID2  = FID(4)
	)

	indexfile1 := NewPageFile(indexFID1, "./data/TestBtreePage_indexfile1.dbf", 1024*1024)
	if err := indexfile1.Create(); err != nil {
		t.Fatalf("File.Create error = %v\n", err)
	}
	indexfile2 := NewPageFile(indexFID2, "./data/TestBtreePage_indexfile2.dbf", 1024*1024)
	if err := indexfile2.Create(); err != nil {
		t.Fatalf("File.Create error = %v\n", err)
	}
	datafile1 := NewPageFile(dataFID1, "./data/TestBtreePage_datafile1.dbf", 1024*1024)
	if err := datafile1.Create(); err != nil {
		t.Fatalf("File.Create error = %v\n", err)
	}
	datafile2 := NewPageFile(dataFID2, "./data/TestBtreePage_datafile2.dbf", 1024*1024)
	if err := datafile2.Create(); err != nil {
		t.Fatalf("File.Create error = %v\n", err)
	}

	getFile := func(fid FID) (*PageFile, error) {
		switch fid {
		case 1:
			return indexfile1, nil
		case 2:
			return indexfile2, nil
		case 3:
			return datafile1, nil
		default:
			return datafile2, nil
		}
	}

	newIndexPageCount := 0
	newIndexPage := func() (*Page, error) {
		newIndexPageCount++
		// Roundrobin
		switch newIndexPageCount % 2 {
		case 1:
			//fmt.Println("newIndexPage:path1")
			return indexfile1.NewPage()
		default:
			//fmt.Println("newIndexPage:path2")
			return indexfile2.NewPage()
		}
	}

	newDataPageCount := 0
	newDataPage := func() (*Page, error) {
		newDataPageCount++
		// Roundrobin
		switch newDataPageCount % 2 {
		case 1:
			return datafile1.NewPage()
		default:
			return datafile2.NewPage()
		}
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
		{order: ascendOrder, elements: 10000, keylen: 16, valuelen: 16},
		{order: descendOrder, elements: 10000, keylen: 16, valuelen: 16},
		{order: randomOrder, elements: 10000, keylen: 16, valuelen: 16},
	}
	//fmt.Printf("ts_idx=%v\n", ts_idx)
	//fmt.Printf("ts_dat=%v\n", ts_dat)
	//fmt.Printf("datafile1=%v\n", datafile1)

	for testNumber, test := range tests {
		fmt.Printf("Testcase[%v]: %v\n", testNumber, test)
		//btree, err := NewBtree(ts_idx, test.keylen, test.valuelen)
		//btree, err := NewBtree(ts_idx.NewPage, pm.GetFile, test.keylen, test.valuelen)
		btree, err := NewBtree(newIndexPage, getFile, test.keylen, test.valuelen)
		if err != nil {
			t.Errorf("Testcase[%v]: NewBtree error:%v", testNumber, err)
		}
		sp, err := NewSlottedPage(newDataPage)
		if err != nil {
			t.Errorf("Testcase[%v]: NewSlottedPage() error:%v", testNumber, err)
		}

		keys := make([][]byte, test.elements)
		values := make([][]byte, test.elements)
		rids := make([]Rid, test.elements)
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

			value := strings.Replace(string(key), "key", "value", 1)
			values[i] = []byte(value)
			//fmt.Printf("keys[%v]=%s values[%v]=%s\n", i, keys[i], i, values[i])

			rid, err := sp.Insert([]byte(value))
			if err != nil {
				t.Errorf("Testcase[%v]: SlottedPage.Insert(%s) error:%v", testNumber, value, err)
				//fmt.Printf("%v", sp)
				sp, err = NewSlottedPage(newDataPage)
				if err != nil {
					t.Errorf("Testcase[%v]: NewSlottedPage() error:%v", testNumber, err)
				}
				rid, err = sp.Insert([]byte(value))
				if err != nil {
					t.Errorf("Testcase[%v]: SlottedPage.Insert(%s) error:%v", testNumber, value, err)
				}
			}
			//t.Logf("rid:%v", rid)
			rids[i] = *rid
		}
		if test.order == randomOrder {
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
				rids[i], rids[j] = rids[j], rids[i]
			})
			//fmt.Printf("keys=%v\n", keys)
			//fmt.Printf("values=%v\n", values)
		}

		//TODO:
		for i, key := range keys {
			//rid := newRid(datafile1, uint32(i), uint16(i))
			err = btree.Insert(key, rids[i])
			//fmt.Printf("Insert keys[%v]=%s values[%v]=%s rids[i]=%v\n", i, keys[i], i, values[i], rids[i])

			if err != nil {
				t.Errorf("Testcase[%v]: Insert error:%v at %s (cycle:%v)", testNumber, err, key, i)
			}
		}
		if !btree.checkLeafKeyOrder() {
			t.Errorf("Testcase[%v]: Leaf keys are not in ascend order.", testNumber)
		}

		for i, key := range keys {
			b, rid := btree.Find(key)
			if !b {
				t.Errorf("Testcase[%v]: No key: %s in B-tree.", testNumber, key)
				continue
			}
			selected_data, err := sp.Select(rid)
			if err != nil {
				t.Errorf("Testcase[%v]: SlottedPage.Select(%s) error:%v", testNumber, key, err)
				continue
			}
			if bytes.Compare(*selected_data, values[i]) != 0 {
				t.Errorf("Testcase[%v]: btree.Find(%s) result %s != %s", testNumber, key, *selected_data, values[i])
				continue
			}
			//fmt.Printf("Find(%s):%s\trid=%v\n", key, *selected_data, *rid)
		}

		for _, original := range btree.walk() {
			data, err := original.ToPageData()
			if err != nil {
				t.Errorf("Testcase[%v]: ToPageData err: %v", testNumber, err)
			}
			//fmt.Printf("PageData: %v\n", data)

			//restored, err := btree.ToNode(data)
			restored, err := data.ToNode(btree)
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
