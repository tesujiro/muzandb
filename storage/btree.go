package storage

import (
	"bytes"
	"errors"
	"fmt"
)

// Btree represents "B+Tree"
type Btree struct {
	tablespace *Tablespace
	keylen     uint8 // bits
	valuelen   uint8
	root       *BtreeNode
	//leafTop         *BtreeNode
	leafCapacity    int
	nonLeafCapacity int
}

// NewBtree returns new "B+tree".
func NewBtree(ts *Tablespace, keylen uint8, valuelen uint8) (*Btree, error) {
	bt := Btree{
		tablespace: ts,
		keylen:     keylen,
		valuelen:   valuelen,
		root:       nil,
		//leafTop:         nil,
		leafCapacity:    int(PageSize-pageHeaderBytes-pagePointerBytes) / int(keylen+ridBytes),
		nonLeafCapacity: int(PageSize-pageHeaderBytes-pagePointerBytes)/int(keylen+pagePointerBytes) - 1,
	}
	return &bt, nil
}

func (bt *Btree) PrintLeaves() {
	for j, key := range bt.root.Keys {
		fmt.Printf("root:\tkey[%v]:%s\n", j, key)
	}

	node := bt.root
	for !node.Leaf {
		node = node.Pointers[0]
	}
	fmt.Printf("leafTop : %v\n", *node)
	for i := 0; ; i++ {
		for j, key := range node.Keys {
			fmt.Printf("node:%v\tkey[%v]:%s\n", i, j, key)
			//_, _ = j, key
		}
		/*
			for j, rid := range node.Rids {
				fmt.Printf("node:%v\trid[%v]:%v\n", i, j, rid)
			}
		*/
		if node.NextLeafNode == nil {
			break
		}
		node = node.NextLeafNode
	}
}

// BtreeNode represents a node for "B+tree"
type BtreeNode struct {
	Parent       *BtreeNode
	Leaf         bool
	Page         *Page
	Capacity     int //Max number of Keys
	Keys         [][]byte
	Rids         []rid        // Only leaf nodes have values
	Pointers     []*BtreeNode // for non leaf nodes
	NextLeafNode *BtreeNode   // for leaf nodes
	//maxPointers  int
}

//func (bt *Btree) newNode(page *Page) *BtreeNode {
func (bt *Btree) newNode() (*BtreeNode, error) {
	page, err := bt.tablespace.NewPage()
	if err != nil {
		return nil, err
	}
	return &BtreeNode{Page: page}, nil
}

func (bt *Btree) newLeafNode() (*BtreeNode, error) {
	node, err := bt.newNode()
	if err != nil {
		return nil, err
	}
	node.Leaf = true
	node.Capacity = bt.leafCapacity
	return node, nil
}

func (bt *Btree) newNonLeafBtreeNode(page *Page) (*BtreeNode, error) {
	node, err := bt.newNode()
	if err != nil {
		return nil, err
	}
	node.Leaf = false
	node.Capacity = bt.nonLeafCapacity
	return node, nil
}

func (bt *Btree) Insert(key []byte, rid rid) error {
	if bt.root == nil {
		node, err := bt.newLeafNode() //??????TODO:
		if err != nil {
			fmt.Println("new page for root node failed")
			return err
		}
		err = node.insertAt(key, rid, 0)
		if err != nil {
			fmt.Printf("insertAt failed:%v\n", err)
			return err
		}
		//fmt.Printf("node=%v\n", node)
		bt.root = node
		//bt.leafTop = node
		return nil
	}
	err := bt.root.insert(key, rid)
	/*
		if err == NodeOverflowError {
			node.split(bt.newLeafNode(page), bt.newLeafNode(page))
		}
	*/
	return err
}

func (node *BtreeNode) newChildNode(keys [][]byte) *BtreeNode {
	return &BtreeNode{
		Parent:   node,
		Leaf:     node.Leaf,
		Capacity: node.Capacity,
		Keys:     keys,
	}
}

func (node *BtreeNode) insert(key []byte, rid rid) error {

	for i, k := range node.Keys {
		switch result := bytes.Compare(key, k); {
		case result < 0:
			if node.Leaf {
				err := node.insertAt(key, rid, i)
				if err == NodeOverflowError && node.Parent == nil {
					node.split()
				}
				return err
			} else {
				err := node.Pointers[i].insert(key, rid)
				if err == NodeOverflowError {
					return node.Pointers[i].split()
				}
				return err
			}
		case result == 0:
			return DuplicateKeyError
		}
	}
	// right most pointer
	if node.Leaf {
		err := node.insertAt(key, rid, len(node.Keys))
		if err == NodeOverflowError && node.Parent == nil {
			return node.split()
		}
		return err
	} else {
		i := len(node.Keys)
		fmt.Printf("i=%v\n", i)
		fmt.Printf("len(node.Pointers)=%v\n", len(node.Pointers))
		err := node.Pointers[i].insert(key, rid)
		if err == NodeOverflowError {
			return node.Pointers[i].split()
		}
		return err
	}

	return nil
}

func (node *BtreeNode) split() error {
	center := node.Capacity / 2
	centerKey := node.Keys[center]

	if node.Parent == nil {
		//
		//   Key1 | Key2 | Key3
		//
		//  ==>
		//          Key2
		//        /      \
		//     Key1  ->  Key2 | Key3
		//
		//
		left := node.newChildNode(node.Keys[:center])
		right := node.newChildNode(node.Keys[center:])
		if node.Leaf {
			left.Rids = node.Rids[:center]
			right.Rids = node.Rids[center:]
			node.Rids = []rid{}
			//fmt.Printf("left.Keys=%v\n", left.Keys)
			//fmt.Printf("right.Keys=%v\n", right.Keys)
			left.NextLeafNode = right
		}

		node.Keys = [][]byte{centerKey}
		node.Leaf = false
		//node.Capacity =   //TODO: change Capacity
		node.Pointers = []*BtreeNode{left, right}
		return nil
	}
	//fmt.Println("set NextLeafNode=right")
	right := node.Parent.newChildNode(node.Keys[center:])
	node.Keys = node.Keys[:center]
	if node.Leaf {
		node.NextLeafNode = right
	}
	err := node.Parent.insertChildNodeByKey(right, centerKey)

	return err
}

func (node *BtreeNode) insertAt(key []byte, rid rid, index int) error {
	if index > len(node.Keys) {
		return errors.New("index out of range.")
	}

	/*
		if index == 0 {
			node.Keys = append([][]byte{key}, node.Keys...)
			if node.Leaf {
				node.Keys = append([][]byte{[]byte(rid)}, node.Keys...)
			}
		} else {
	*/
	node.Keys = append(node.Keys, key) // extend one element
	copy(node.Keys[index+1:], node.Keys[index:])
	node.Keys[index] = key
	//newKeys := append(node.Keys[:index], key)
	//newKeys = append(newKeys, node.Keys[index:]...)
	//node.Keys = newKeys
	if node.Leaf {
		node.Rids = append(node.Rids, rid) // extend one element
		copy(node.Rids[index+1:], node.Rids[index:])
		node.Rids[index] = rid
		//newRids := append(node.Rids[:index], rid)
		//newRids = append(newRids, node.Rids[index:]...)
		//node.Rids = newRids
	}
	//}
	if len(node.Keys) > node.Capacity {
		return NodeOverflowError
	}
	return nil
}

func (node *BtreeNode) insertChildNodeByKey(child *BtreeNode, key []byte) error {
	index := len(node.Keys)
	for i, k := range node.Keys {
		if bytes.Compare(k, key) >= 0 {
			index = i
			break
		}
	}
	node.Keys = append(node.Keys, key) // extend one element
	copy(node.Keys[index+1:], node.Keys[index:])
	node.Keys[index] = key
	//newKeys := append(node.Keys[:index], key)
	//newKeys = append(newKeys, node.Keys[index:]...)
	//node.Keys = newKeys

	node.Pointers = append(node.Pointers, child) // extend one element
	copy(node.Pointers[index+1:], node.Pointers[index:])
	node.Pointers[index] = child
	//newPointers := append(node.Pointers[:index], child)
	//newPointers = append(newPointers, node.Pointers[index:]...)
	//node.Pointers = newPointers

	if len(node.Keys) > node.Capacity {
		return NodeOverflowError
	}
	return nil
}

func (bt *Btree) Find(key []byte) (bool, int) {
	if bt.root == nil {
		return false, 0
	}
	return bt.root.find(key)
}

func (node *BtreeNode) find(key []byte) (bool, int) {
	for i, k := range node.Keys {
		if bytes.Compare(key, k) == 0 {
			return true, i
		}
		if bytes.Compare(key, k) < 0 {
			return node.Pointers[i].find(key)
		}
	}
	if node.Leaf {
		return false, 0
	}
	return node.Pointers[len(node.Keys)].find(key)
}

/*
func (bt *Btree) Delete(key []byte) error {
}

func (bt *Btree) SearchRange(key1, key2 []byte) error {
}
*/
