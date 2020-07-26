package storage

import (
	"bytes"
	"fmt"
)

// Btree represents "B+Tree"
type Btree struct {
	tablespace  *Tablespace
	keylen      uint8 // bits
	root        *BtreeNode
	leafTop     *BtreeNode
	maxBranches int
}

// BtreeNode represents a node for "B+tree"
type BtreeNode struct {
	leaf        bool
	page        *Page
	keys        [][]byte
	pointers    []*BtreeNode
	maxBranches int
}

func (bt *Btree) newLeafBtreeNode(page *Page) *BtreeNode {
	maxBranches := int(PageSize-pageHeaderBytes-pagePointerBytes)/int(bt.keylen+ridBytes) + 1
	return &BtreeNode{leaf: true, page: page, maxBranches: maxBranches}
}

func (bt *Btree) newNonLeafBtreeNode(page *Page) *BtreeNode {
	maxBranches := int(PageSize-pageHeaderBytes-pagePointerBytes)/int(bt.keylen+pagePointerBytes) + 1
	return &BtreeNode{leaf: false, page: page, maxBranches: maxBranches}
}

// NewBterr returns new "B+tree".
func NewBtree(ts *Tablespace, keylen uint8) (*Btree, error) {

	bt := Btree{
		tablespace: ts,
		keylen:     keylen,
		root:       nil,
		leafTop:    nil,
	}
	return &bt, nil
}

func (bt *Btree) Insert(key []byte, rid rid) error {
	if bt.root == nil {
		page, err := bt.tablespace.NewPage()
		if err != nil {
			fmt.Println("new page for root node failed")
			return err
		}
		node := bt.newLeafBtreeNode(page)
		node.insertAt(key, rid, 0)
		bt.root = node
		bt.leafTop = node
		return nil
	}
	return bt.root.insert(key, rid)
}

func (node *BtreeNode) insert(key []byte, rid rid) error {
	ok, index := node.find(key)
	if ok {
		return DuplicateKeyError
	}
	//if len(node.pointers) == 0 {
	if node.leaf {
		node.insertAt(key, rid, index)
		if len(node.keys) == node.maxBranches {
			node.split()
		}
		return nil
	}

	//if len(node.keys) >
	////////////
	////////////
	////////////
	////////////
	////////////

	return nil

}

func (node *BtreeNode) insertAt(key []byte, rid rid, index int) error {
	page := node.page

	value := append(key, rid.Bytes()...)
	if int(page.header.slots) < index {
		return fmt.Errorf("insertAt error: index larger than slots")
	}
	if page.header.slots == 0 {
		_, err := page.InsertRecord(value)
		return err
	}
	for i := node.page.header.slots; i > uint16(index); i-- {
		record, err := page.SelectRecord(i - 1)
		if err != nil {
			return err
		}
		if i == page.header.slots {
			page.InsertRecord(record)
		} else {
			page.UpdateRecord(i, record)
		}
	}
	page.UpdateRecord(uint16(index), value)

	return nil
}

func (bt *Btree) Find(key []byte) (bool, int) {
	if bt.root == nil {
		return false, -1
	}
	return bt.root.find(key)
}

func (node *BtreeNode) find(key []byte) (bool, int) {
	for i, k := range node.keys {
		if bytes.Compare(key, k) == 0 {
			return true, i
		}
		if bytes.Compare(key, k) < 0 {
			return node.pointers[i].find(key)
		}
	}
	if node.leaf {
		return false, -1
	}
	return node.pointers[len(node.keys)].find(key)
}

/*
func (bt *Btree) Delete(key []byte) error {
}

func (bt *Btree) SearchRange(key1, key2 []byte) error {
}
*/
