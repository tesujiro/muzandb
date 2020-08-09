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
	fmt.Printf("root:")
	printKeys(bt.root.Keys)
	for i, node := range bt.root.Pointers {
		fmt.Printf("node[%v]:", i)
		printKeys(node.Keys)
	}

	node := bt.root
	fmt.Printf("root:\n")
	for !node.Leaf {
		fmt.Printf("keys: ")
		printKeys(node.Keys)
		fmt.Printf("->node.Pointers(len:%v):", len(node.Pointers))
		fmt.Printf("\n")
		node = node.Pointers[0]
	}
	//fmt.Printf("leafTop : %v\n", *node)
	count := 0
	var prev []byte
	for i := 0; ; i++ {
		//fmt.Printf("node:%v len(keys):%v\n", i, len(node.Keys))
		/*
			fmt.Printf("node:%v len(keys):%v len(parent.Keys):%v", i, len(node.Keys), len(node.Parent.Keys))
			for _, key := range node.Parent.Keys {
				fmt.Printf(" %s", key)
			}
			fmt.Printf("\n")
		*/

		for j, key := range node.Keys {
			if bytes.Compare(key, prev) < 0 {
				fmt.Printf("*** node:%v\tkey[%v]:%s prev:%s\n", i, j, key, prev)
			}
			fmt.Printf("node:%v\tkey[%v]:%s\n", i, j, key)
			prev = key
			count++
			//_, _ = j, key
		}
		/*
			fmt.Printf("Panrent.Keys:")
			printKeys(node.Parent.Keys)
			fmt.Printf("  parent: %v\n", node.Parent)
		*/
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
	fmt.Printf("key count:%v\n", count)
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

func printKeys(keys [][]byte) {
	for _, key := range keys {
		fmt.Printf(" %s", key)
	}
	fmt.Printf("\n")
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

func (node *BtreeNode) overflow() bool {
	if node == nil {
		return false
	}
	return len(node.Keys) > node.Capacity
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
	if err != nil {
		fmt.Printf("insert failed:%v\n", err)
		return err
	}
	if bt.root.overflow() {
		fmt.Println("Insert -> split()")
		return bt.root.split()
	}
	return nil
}

func (node *BtreeNode) newChildNode(keys [][]byte) *BtreeNode {
	newKeys := make([][]byte, len(keys))
	copy(newKeys, keys)
	fmt.Println("newChlidKey")
	fmt.Printf("set Parent: %v\n", node)
	fmt.Printf("set Keys:")
	printKeys(keys)
	return &BtreeNode{
		Parent:   node,
		Leaf:     node.Leaf,
		Capacity: node.Capacity,
		Keys:     newKeys,
	}
}

func (node *BtreeNode) insert(key []byte, rid rid) error {

	fmt.Printf("+++ insert key: %s --> node keys:", key)
	printKeys(node.Keys)
	if node == nil {
		fmt.Println("node==nil")
	}
	if !node.Leaf {
		if len(node.Keys)+1 != len(node.Pointers) {
			fmt.Printf("insert(%s) WARNING len(node.Keys):%v len(node.Pointes):%v\n", key, len(node.Keys), len(node.Pointers))
		}
	}

	for i, k := range node.Keys {
		switch result := bytes.Compare(key, k); {
		case result < 0:
			if node.Leaf {
				fmt.Printf("insertAt 1 \n")
				err := node.insertAt(key, rid, i)
				if err != nil {
					return err
				}
			} else {
				fmt.Println("insert 1")
				err := node.Pointers[i].insert(key, rid)
				if err != nil {
					return err
				}
				//fmt.Printf("split 2 node.Pointers[%v].Keys=%v\n", i, len(node.Pointers[i].Keys))
				if node.Pointers[i].overflow() {
					err := node.Pointers[i].split()
					if err != nil {
						return err
					}
				}
			}
			if node.overflow() {
				return node.split()
			}
			return nil
		case result == 0:
			return DuplicateKeyError
		}
	}
	fmt.Printf("right most pointer for %s\n", key)
	// right most pointer
	if node.Leaf {
		fmt.Printf("insertAt 2 key=%s\n", key)
		err := node.insertAt(key, rid, len(node.Keys))
		if err != nil {
			return err
		}
		fmt.Printf(" --> keys:")
		printKeys(node.Keys)
		//if err == NodeOverflowError && node.Parent == nil {
		//if err == NodeOverflowError {
		if node.overflow() {
			fmt.Println("split 3")
			return node.split()
		}
		return err
	} else {
		i := len(node.Keys)
		/*
			fmt.Printf("insert key= %s\n", key)
			fmt.Printf("node.Keys(len:%v)= %s - %s\n", len(node.Keys), node.Keys[0], node.Keys[len(node.Keys)-1])
			fmt.Printf("node.Pointers(len:%v)\n", len(node.Pointers))
			fmt.Printf("node.Pointers[%v].insert(%s, rid)\n", i, key)
			for j, k := range node.Pointers[i].Keys {
				fmt.Printf("Key[%v]=%s ", j, k)
			}
			fmt.Printf("\n")
		*/
		if len(node.Pointers) < i+1 {
			//return fmt.Errorf("node.Pointers length %v too short for len(node.Keys)=%v \n", len(node.Pointers), i)
			fmt.Printf("node.Pointers length %v too short for len(node.Keys)=%v \n", len(node.Pointers), i)
		}
		fmt.Printf("insert 2 i=%v key=%s\n", i, key)
		err := node.Pointers[i].insert(key, rid)
		if err != nil {
			return err
		}
		//if err == NodeOverflowError {
		if len(node.Pointers) > i && node.Pointers[i].overflow() {
			fmt.Printf("insert 2 -> node.Pointer[i].NodeOverflowError\n")
			fmt.Printf("node.Pointers[%v].Keys:", i)
			printKeys(node.Pointers[i].Keys)
			//fmt.Printf("len(node.Pointers[%v].Keys)=%v\n", i, len(node.Pointers[i].Keys))
			//fmt.Printf("split 4 len(node.Pointers[%v].Keys)=%v\n", i, len(node.Pointers[i].Keys))
			err2 := node.Pointers[i].split()
			if err2 != nil {
				return err2
			}
		}
		if node.overflow() {
			return node.split()
		}
		return nil
	}

	return nil
}

func (node *BtreeNode) split() error {
	fmt.Printf("++++++ split keys:")
	printKeys(node.Keys)
	center := node.Capacity / 2
	centerKey := node.Keys[center]
	fmt.Printf("node.Capacity=%v\tlen(node.Keys)=%v centerKey=%s\n", node.Capacity, len(node.Keys), centerKey)

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
		//node.Capacity =   //TODO: change Capacity
		node.Keys = [][]byte{centerKey}
		if node.Leaf {
			node.Leaf = false
			left.Rids = node.Rids[:center]
			right.Rids = node.Rids[center:]
			node.Rids = []rid{}
			left.NextLeafNode = right
		} else {
			//
			//  P0 | Key0 | P1 | Key1 | P2 | Keys2 | P3
			//
			// ==>
			//                Key1
			//            /           \
			//  P0 | Key0 | P1      NIL | Key1 | P2 | Keys2 | P3
			//
			left.Pointers = make([]*BtreeNode, len(node.Pointers[:center+1]))
			copy(left.Pointers, node.Pointers[:center+1])
			for _, child := range left.Pointers {
				child.Parent = left
			}

			right.Pointers = make([]*BtreeNode, len(node.Pointers[center:]))
			copy(right.Pointers, node.Pointers[center:])
			right.Pointers[0] = nil
			for _, child := range right.Pointers[1:] {
				child.Parent = right
			}
		}
		node.Pointers = []*BtreeNode{left, right}

		return nil
	}
	//fmt.Println("split()=>NOT root")
	//fmt.Println("set NextLeafNode=right")
	fmt.Println("XXXXXXXXXXXXXXXXXXXXXXXXX")
	fmt.Printf("  parent keys:")
	printKeys(node.Parent.Keys)
	fmt.Printf("  parent: %v\n", node.Parent)
	fmt.Printf("  node keys:")
	printKeys(node.Keys)
	fmt.Println("XXXXXXXXXXXXXXXXXXXXXXXXX")
	right := node.Parent.newChildNode(node.Keys[center:])
	//right.Parent = node.Parent
	//node.Keys = node.Keys[:center]
	//copy(node.Keys, node.Keys[:center])
	newKeys := make([][]byte, len(node.Keys[:center]))
	copy(newKeys, node.Keys[:center])
	node.Keys = newKeys
	right.Leaf = node.Leaf
	if node.Leaf {
		newRids := make([]rid, len(node.Rids[center:]))
		copy(newRids, node.Rids[center:])
		right.Rids = newRids
		//TODO: change node.Rids
		right.NextLeafNode = node.NextLeafNode
		node.NextLeafNode = right
	} else {
		right.Pointers = append([]*BtreeNode{nil}, node.Pointers[center+1:]...)
		//node.Pointers = node.Pointers[:center+1]
		node.Pointers = make([]*BtreeNode, len(node.Pointers[:center+1]))
		copy(node.Pointers, node.Pointers[:center+1])
		for _, child := range right.Pointers {
			if child != nil {
				child.Parent = right
			}
		}
		fmt.Printf("After split left pointers(len:%v) right pointers(len:%v)\n", len(node.Pointers), len(right.Pointers))
	}
	//fmt.Printf("Node Keys: %s - %s\n", node.Keys[0], node.Keys[len(node.Keys)-1])
	//fmt.Printf("new ChildNode Keys: %s - %s\n", right.Keys[0], right.Keys[len(right.Keys)-1])
	fmt.Println("YYYYYYYYYYYYYYYYYYYYYYYYY")
	fmt.Printf("before insertChildNodeByKey -> centerKey=%s\n", centerKey)
	fmt.Printf("  parent keys:")
	printKeys(node.Parent.Keys)
	fmt.Printf("  parent: %v\n", node.Parent)
	fmt.Printf("  right keys:")
	printKeys(right.Keys)
	fmt.Printf("  rihgt: %v\n", right)
	fmt.Printf("  node : %v\n", node)
	fmt.Printf("  rihgt.parent: %v\n", right.Parent)
	fmt.Printf("  node.parent : %v\n", node.Parent)
	fmt.Println("YYYYYYYYYYYYYYYYYYYYYYYYY")
	err := node.Parent.insertChildNodeByKey(right, centerKey)
	if err != nil {
		return err
	}
	if node.Parent.overflow() {
		return node.Parent.split()
	}
	return nil
	//fmt.Printf("after insertChildNodeByKey -> len(Parent.node.Keys)=%v\n", len(node.Parent.Keys))
	//fmt.Printf("after insertChildNodeByKey -> len(node.Keys)=%v\n", len(node.Keys))
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

	return nil
}

func (node *BtreeNode) insertChildNodeByKey(child *BtreeNode, key []byte) error {
	index := len(node.Keys)
	for i, k := range node.Keys {
		if bytes.Compare(k, key) >= 0 {
			index = i
			break
		}
		//TODO: if equal --> duplicate key error
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
	copy(node.Pointers[ptrIndex+1:], node.Pointers[ptrIndex:])
	node.Pointers[ptrIndex] = child

	fmt.Printf("insertChildNodeByKey -> len(node.Keys)=%v\n", len(node.Keys))
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
