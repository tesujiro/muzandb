package storage

import "fmt"

type DB struct {
	btree *Btree
	sp    *SlottedPage
}

func OpenFile(filepath string) (*DB, error) {
	pm := startPageManager()
	indexfile1 := pm.NewFile("./data/TestBtreePage_indexfile1.dbf", 1024*1024)
	datafile1 := pm.NewFile("./data/TestBtreePage_datafile1.dbf", 1024*1024)

	ts_idx, err := pm.NewTablespace("INDEXSPACE1")
	if err != nil {
		return nil, fmt.Errorf("PageManger.newTablespace() error:%v", err)
	}

	ts_dat, err := pm.NewTablespace("DATASPACE1")
	if err != nil {
		return nil, fmt.Errorf("PageManger.newTablespace() error:%v", err)
	}

	err = ts_idx.addFile(indexfile1)
	if err != nil {
		return nil, fmt.Errorf("Tablespace.addFile(%v) error:%v", indexfile1, err)
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
	return nil
}

func (db *DB) Put(key, value []byte) error {
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return nil, nil
}
