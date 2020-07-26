package storage

import (
	"bytes"
	"fmt"
)

// Btree represents "B+Tree"
type Btree struct {
	tablespace *Tablespace
	keylen     uint8 // bits
	root       *BtreeNode
	leafTop    *BtreeNode
	//branches   uint16
}

// BtreeNode represents a node for "B+tree"
type BtreeNode struct {
	leaf     bool
	page     *Page
	keys     [][]byte
	pointers []*BtreeNode
}

func newLeafBtreeNode(page *Page) *BtreeNode {
	return &BtreeNode{leaf: true, page: page}
}

func newNonLeafBtreeNode(page *Page) *BtreeNode {
	return &BtreeNode{leaf: false, page: page}
}

// NewBterr returns new "B+tree".
func NewBtree(ts *Tablespace, keylen uint8) (*Btree, error) {
	//maxBranches := uint16(PageSize-pageHeaderBytes-pagePointerBytes)/uint16(keylen+pagePointerBytes) + 1

	bt := Btree{
		tablespace: ts,
		keylen:     keylen,
		root:       nil,
		leafTop:    nil,
		//branches:   maxBranches,
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
		node := newLeafBtreeNode(page)
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
	if len(node.pointers) == 0 {
		node.insertAt(key, rid, index)
		////////////
		////////////
		////////////
		////////////
		return nil
	}
	////////////
	////////////
	////////////
	////////////
	////////////

	return nil

}

func (node *BtreeNode) insertAt(key []byte, rid rid, index int) error {
	//if

	return nil
}

func (bt *Btree) Find(key []byte) (bool, int) {
	return bt.root.find(key)
}

func (node *BtreeNode) find(key []byte) (bool, int) {
	for i, k := range node.keys {
		if bytes.Compare(key, k) < 0 {
			return node.pointers[i].find(key)
		}
	}
	return node.pointers[len(node.pointers)-1].find(key)
}

/*
func (bt *Btree) Delete(key []byte) error {
}

func (bt *Btree) SearchRange(key1, key2 []byte) error {
}
*/
