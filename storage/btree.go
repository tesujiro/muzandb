package storage

import (
	"bytes"
	"fmt"
)

// Btree represents "B+Tree"
type Btree struct {
	tablespace *Tablespace
	keylen     uint8 // bits
	valuelen   uint8
	root       *BtreeNode
	leafTop    *BtreeLeafNode
}

// NewBtree returns new "B+tree".
func NewBtree(ts *Tablespace, keylen uint8, valuelen uint8) (*Btree, error) {
	bt := Btree{
		tablespace: ts,
		keylen:     keylen,
		valuelen:   valuelen,
		root:       nil,
		leafTop:    nil,
	}
	return &bt, nil
}

// BtreeNode represents a node for "B+tree"
type BtreeNode struct {
	Leaf        bool
	Page        *Page
	maxBranches int
}

type BtreeLeafNode struct {
	BtreeNode
	Values [][]byte
	Next   *BtreeLeafNode
}

type BtreeNonLeafNode struct {
	BtreeNode
	KeyCapacity int
	Keys        [][]byte
	Pointers    []*BtreeNode
}

func (bt *Btree) newLeafBtreeNode(page *Page) *BtreeLeafNode {
	maxBranches := int(PageSize-pageHeaderBytes-pagePointerBytes)/int(bt.keylen+ridBytes) + 1
	return &BtreeLeafNode{Leaf: true, Page: page, maxBranches: maxBranches}
}

func (bt *Btree) newNonLeafBtreeNode(page *Page) *BtreeNonLeafNode {
	maxBranches := int(PageSize-pageHeaderBytes-pagePointerBytes)/int(bt.keylen+pagePointerBytes) + 1
	return &BtreeNonLeafNode{Leaf: false, Page: page, maxBranches: maxBranches}
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
	if node.Leaf {
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

// can stick out from a page ??h
func (node *BtreeNode) insertAt(key []byte, rid rid, index uint16) error {
	value := append(key, rid.Bytes()...)
	return node.page.InsertRecordAt(value, index)
}

func (bt *Btree) Find(key []byte) (bool, uint16) {
	if bt.root == nil {
		return false, 0
	}
	return bt.root.find(key)
}

func (node *BtreeNode) find(key []byte) (bool, uint16) {
	for i, k := range node.keys {
		if bytes.Compare(key, k) == 0 {
			return true, uint16(i)
		}
		if bytes.Compare(key, k) < 0 {
			return node.pointers[i].find(key)
		}
	}
	if node.Leaf {
		return false, 0
	}
	return node.pointers[len(node.keys)].find(key)
}

/*
func (bt *Btree) Delete(key []byte) error {
}

func (bt *Btree) SearchRange(key1, key2 []byte) error {
}
*/
