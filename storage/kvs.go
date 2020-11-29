package storage

import (
	"fmt"
	"os"
)

type DB struct {
	btree *Btree
	sp    *SlottedPage
}

func OpenFile(filepath string) (*DB, error) {
	//fmt.Println("OpenFile(" + filepath + ")")
	pm := startPageManager()
	//TODO: path
	indexfile1 := pm.NewFile(filepath+"/KVS_indexfile1.dbf", 1024*4096*20)
	datafile1 := pm.NewFile(filepath+"/KVS_datafile1.dbf", 1024*4096*20)

	ts_idx, err := pm.NewTablespace("INDEXSPACE1")
	if err != nil {
		return nil, fmt.Errorf("PageManger.newTablespace() error:%v", err)
	}

	ts_dat, err := pm.NewTablespace("DATASPACE1")
	if err != nil {
		return nil, fmt.Errorf("PageManger.newTablespace() error:%v", err)
	}

	err = ts_idx.addFile(indexfile1)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("Tablespace.addFile(%v) error:%T %v", indexfile1, err, err)
	}
	err = ts_dat.addFile(datafile1)
	if err != nil {
		return nil, fmt.Errorf("Tablespace.addFile(%v) error:%v", datafile1, err)
	}

	btree, err := NewBtree(ts_idx, 32, 128) //TODO: keylen, valuelen
	if err != nil {
		return nil, fmt.Errorf("NewBtree error:%v", err)
	}
	sp, err := newSlottedPage(ts_dat)
	if err != nil {
		return nil, fmt.Errorf("newSlottedPage() error:%v", err)
	}

	return &DB{btree: btree, sp: sp}, nil
}

func (db *DB) Close() error {
	// TODO:
	return nil
}

func (db *DB) Put(key, value []byte) error {
	rid, err := db.sp.Insert([]byte(value))
	if err != nil {
		return fmt.Errorf("SlottedPage.Insert(%s) error:%v", value, err)
		/* TODO: ??
		//fmt.Printf("%v", sp)
		db.sp, err = newSlottedPage(ts_dat)
		if err != nil {
			return fmt.Errorf("Testcase[%v]: newSlottedPage() error:%v", err)
		}
		rid, err = sp.Insert([]byte(value))
		if err != nil {
			return fmt.Errorf("Testcase[%v]: SlottedPage.Insert(%s) error:%v", value, err)
		}
		*/
	}

	err = db.btree.Insert(key, *rid)
	//fmt.Printf("Insert keys[%v]=%s values[%v]=%s rids[i]=%v\n", i, keys[i], i, values[i], rids[i])

	if err != nil {
		return fmt.Errorf("Btree.Insert error:%T %v at %s ", err, err, key)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	b, rid := db.btree.Find(key)
	if !b {
		// TODO: new error?
		return nil, fmt.Errorf("No key: %s in B-tree.", key)
	}
	selected_data, err := db.sp.Select(rid)
	if err != nil {
		return nil, fmt.Errorf(" SlottedPage.Select(%s) error:%v", key, err)
	}
	return *selected_data, nil
}
