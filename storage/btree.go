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
	/*
		for j, key := range bt.root.Keys {
			fmt.Printf("root:\tkey[%v]:%s\n", j, key)
		}
	*/

	node := bt.root
	for !node.Leaf {
		//fmt.Printf("node.Pointers(len:%v)\n", len(node.Pointers))
		node = node.Pointers[0]
	}
	fmt.Printf("leafTop : %v\n", *node)
	for i := 0; ; i++ {
		fmt.Printf("node:%v len(keys):%v\n", i, len(node.Keys))
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
	if err == NodeOverflowError {
		fmt.Println("Insert -> split()")
		return bt.root.split()
	}
	return err
}

func (node *BtreeNode) newChildNode(keys [][]byte) *BtreeNode {
	newKeys := make([][]byte, len(keys))
	copy(newKeys, keys)
	return &BtreeNode{
		Parent:   node,
		Leaf:     node.Leaf,
		Capacity: node.Capacity,
		Keys:     newKeys,
	}
}

func (node *BtreeNode) insert(key []byte, rid rid) error {

	if !node.Leaf {
		if len(node.Keys)+1 != len(node.Pointers) {
			fmt.Printf("insert(%s) WARNING len(node.Keys):%v len(node.Pointes):%v\n", key, len(node.Keys), len(node.Pointers))
		}
	}

	for i, k := range node.Keys {
		switch result := bytes.Compare(key, k); {
		case result < 0:
			if node.Leaf {
				err := node.insertAt(key, rid, i)
				if err == NodeOverflowError && node.Parent == nil {
					fmt.Println("split 1")
					return node.split()
				}
				return err
			} else {
				err := node.Pointers[i].insert(key, rid)
				fmt.Printf("split 2 node.Pointers[%v].Keys=%v\n", i, len(node.Pointers[i].Keys))
				for err == NodeOverflowError {
					err = node.Pointers[i].split()
				}
				/*
					if err == NodeOverflowError {
						fmt.Printf("split 2 node.Pointers[%v].Keys=%v\n", i, len(node.Pointers[i].Keys))
						err2 := node.Pointers[i].split()
						fmt.Printf("split 2 return=%v\n", err2)
						return err2
					}
				*/
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
			fmt.Println("split 3")
			return node.split()
		}
		return err
	} else {
		i := len(node.Keys)
		fmt.Printf("insert key= %s\n", key)
		fmt.Printf("node.Keys(len:%v)= %s - %s\n", len(node.Keys), node.Keys[0], node.Keys[len(node.Keys)-1])
		fmt.Printf("node.Pointers(len:%v)\n", len(node.Pointers))
		fmt.Printf("node.Pointers[%v].insert(%s, rid)\n", i, key)
		if len(node.Pointers) < i+1 {
			//return fmt.Errorf("node.Pointers length %v too short for len(node.Keys)=%v \n", len(node.Pointers), i)
			fmt.Printf("node.Pointers length %v too short for len(node.Keys)=%v \n", len(node.Pointers), i)
		}
		err := node.Pointers[i].insert(key, rid)
		if err == NodeOverflowError {
			//fmt.Printf("len(node.Pointers[%v].Keys)=%v\n", i, len(node.Pointers[i].Keys))
			//fmt.Printf("split 4 len(node.Pointers[%v].Keys)=%v\n", i, len(node.Pointers[i].Keys))
			err2 := node.Pointers[i].split()
			if err2 == NodeOverflowError && node.Parent == nil {
				fmt.Printf("split 5\n")
				return node.split()
			}
			return err2
		}
		return err
	}

	return nil
}

func (node *BtreeNode) split() error {
	//fmt.Println("split()")
	center := node.Capacity / 2
	fmt.Printf("node.Capacity=%v\tlen(node.Keys)=%v\n", node.Capacity, len(node.Keys))
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
		//fmt.Println("split()=>root")
		left := node.newChildNode(node.Keys[:center])
		right := node.newChildNode(node.Keys[center:])
		if node.Leaf {
			left.Rids = node.Rids[:center]
			right.Rids = node.Rids[center:]
			node.Rids = []rid{}
			//fmt.Printf("left.Keys=%v\n", left.Keys)
			//fmt.Printf("right.Keys=%v\n", right.Keys)
			left.NextLeafNode = right
		} else {
			//fmt.Println("split()=>root && Non Leaf")
			//fmt.Printf("len(node.Pointers)=%v\n", len(node.Pointers))
			//left.Pointers = node.Pointers[:center]
			//left.Pointers = node.Pointers[:center+1]
			left.Pointers = make([]*BtreeNode, len(node.Pointers[:center+1]))
			copy(left.Pointers, node.Pointers[:center+1])
			right.Pointers = make([]*BtreeNode, len(node.Pointers[center:]))
			copy(right.Pointers, node.Pointers[center:])
			right.Pointers[0] = nil
			//right.Pointers = node.Pointers[center+1:]
			//right.Pointers = append([]*BtreeNode{nil}, node.Pointers[center+1:]...)
		}

		node.Keys = [][]byte{centerKey}
		node.Leaf = false
		//node.Capacity =   //TODO: change Capacity
		node.Pointers = []*BtreeNode{left, right}
		return nil
	}
	//fmt.Println("split()=>NOT root")
	//fmt.Println("set NextLeafNode=right")
	right := node.Parent.newChildNode(node.Keys[center:])
	//node.Keys = node.Keys[:center]
	//copy(node.Keys, node.Keys[:center])
	newKeys := make([][]byte, len(node.Keys[:center]))
	copy(newKeys, node.Keys[:center])
	node.Keys = newKeys
	right.Leaf = node.Leaf
	if right.Leaf {
		right.Rids = node.Rids[center:]
		right.NextLeafNode = node.NextLeafNode
	} else {
		right.Pointers = append([]*BtreeNode{nil}, node.Pointers[center+1:]...)
		node.Pointers = node.Pointers[:center+1]
		fmt.Printf("After split left pointers(len:%v) right pointers(len:%v)\n", len(node.Pointers), len(right.Pointers))
	}
	//fmt.Printf("Node Keys: %s - %s\n", node.Keys[0], node.Keys[len(node.Keys)-1])
	//fmt.Printf("new ChildNode Keys: %s - %s\n", right.Keys[0], right.Keys[len(right.Keys)-1])
	if node.Leaf {
		node.NextLeafNode = right
	}
	err := node.Parent.insertChildNodeByKey(right, centerKey)
	//fmt.Printf("after insertChildNodeByKey -> len(Parent.node.Keys)=%v\n", len(node.Parent.Keys))
	//fmt.Printf("after insertChildNodeByKey -> len(node.Keys)=%v\n", len(node.Keys))
	for err == NodeOverflowError {
		err = node.Parent.split()
	}

	return err
}

func (node *BtreeNode) insertAt(key []byte, rid rid, index int) error {
	if index > len(node.Keys) {
		return errors.New("index out of range.")
	}
	//fmt.Printf("Key=%s at insertAt %v\n", key, index)
	node.Keys = append(node.Keys, key) // extend one element
	copy(node.Keys[index+1:], node.Keys[index:])
	node.Keys[index] = key

	if node.Leaf {
		node.Rids = append(node.Rids, rid) // extend one element
		copy(node.Rids[index+1:], node.Rids[index:])
		node.Rids[index] = rid
	}

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
	fmt.Printf("insertChildNodeByKey(child,%s)\n", key)
	//fmt.Printf("node.Keys(len:%v): %s - %s\n", len(node.Keys), node.Keys[0], node.Keys[len(node.Keys)-1])
	//fmt.Printf("index=%v\n", index)
	//fmt.Printf("len(node.Pointers)=%v\n", len(node.Pointers))

	// insert key
	node.Keys = append(node.Keys, key) // extend one element
	copy(node.Keys[index+1:], node.Keys[index:])
	node.Keys[index] = key

	// insert child pointer
	ptrIndex := index + 1
	node.Pointers = append(node.Pointers, child) // extend one element
	copy(node.Pointers[ptrIndex:], node.Pointers[ptrIndex:])
	node.Pointers[ptrIndex] = child

	fmt.Printf("insertChildNodeByKey -> len(node.Keys)=%v\n", len(node.Keys))
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
