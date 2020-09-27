package storage

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/tesujiro/muzan/debug"
)

// Btree represents "B+Tree"
type Btree struct {
	tablespace      *Tablespace
	keylen          uint8 // bits
	valuelen        uint8
	root            *BtreeNode
	leafCapacity    int
	nonLeafCapacity int
}

// NewBtree returns new "B+tree".
func NewBtree(ts *Tablespace, keylen uint8, valuelen uint8) (*Btree, error) {
	bt := Btree{
		tablespace:      ts,
		keylen:          keylen,
		valuelen:        valuelen,
		root:            nil,
		leafCapacity:    int(PageSize-pageHeaderBytes-pagePointerBytes) / int(keylen+ridBytes),
		nonLeafCapacity: int(PageSize-pageHeaderBytes-pagePointerBytes)/int(keylen+pagePointerBytes) - 1,
	}
	return &bt, nil
}

// BtreeNode represents a node for "B+tree"
type BtreeNode struct {
	Tablespace *Tablespace
	Page       *Page
	Parent     BtreeNodePtr
	Leaf       bool
	Capacity   int          //Max number of Keys
	NextLeaf   BtreeNodePtr //for leaf nodes
	Keys       [][]byte
	Rids       []rid          // Only leaf nodes have values
	Pointers   []BtreeNodePtr // for non leaf nodes
	Updated    bool
	//maxPointers  int
}

func (node BtreeNode) String() string {
	var s string
	s = fmt.Sprintf("\n")
	s = fmt.Sprintf("%vTablespace:\t%v\n", s, node.Tablespace)
	s = fmt.Sprintf("%vPage:\t%v\n", s, node.Page)
	s = fmt.Sprintf("%vParent:\t%v\n", s, node.Parent)
	s = fmt.Sprintf("%vLeaf:\t%v\n", s, node.Leaf)
	s = fmt.Sprintf("%vCapacity:\t%v\n", s, node.Capacity)
	s = fmt.Sprintf("%vNextLeaf:\t%v\n", s, node.NextLeaf)
	s = fmt.Sprintf("%vKeys:\t%s\n", s, node.Keys)
	s = fmt.Sprintf("%vRids:\t%v\n", s, node.Rids)
	s = fmt.Sprintf("%vPointers:\t%v\n", s, node.Pointers)
	s = fmt.Sprintf("%vUpdated:\t%v\n", s, node.Updated)
	return s
}

type BtreeNodePtr struct {
	page *Page
	node *BtreeNode
}

func (ptr BtreeNodePtr) String() string {
	return fmt.Sprintf("%v", ptr.page)
}

func (ptr *BtreeNodePtr) GetNode() (*BtreeNode, error) {
	if ptr.node != nil {
		return ptr.node, nil
	}
	if ptr.page != nil {
		// get node from page
		// TODO: READ PAGE
	}
	return nil, nil
}

func (ptr *BtreeNodePtr) GetPage() *Page {
	return ptr.page
}

func (ptr *BtreeNodePtr) SetNode(node *BtreeNode) {
	ptr.node = node
}

func (ptr *BtreeNodePtr) SetPage(page *Page) {
	ptr.page = page
}

func printKeys(keys [][]byte) {
	for _, key := range keys {
		debug.Printf(" %s", key)
	}
	debug.Printf("\n")
}

func (bt *Btree) newRootNode() (*BtreeNode, error) {
	page, err := bt.tablespace.NewPage()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Tablepace.NewPage()=%v\n", page)
	return &BtreeNode{
		Tablespace: bt.tablespace,
		Page:       page,
		Leaf:       true,
		Capacity:   bt.leafCapacity,
	}, nil
}

func (node *BtreeNode) newChildNode(keys [][]byte) (*BtreeNode, error) {
	newKeys := make([][]byte, len(keys))
	copy(newKeys, keys)

	page, err := node.Tablespace.NewPage()
	if err != nil {
		return nil, err
	}
	//fmt.Printf("Tablepace.NewPage()=%v\n", page)
	return &BtreeNode{
		Tablespace: node.Tablespace,
		Page:       page,
		Parent:     BtreeNodePtr{node: node, page: node.Page},
		Leaf:       node.Leaf,
		Capacity:   node.Capacity,
		Keys:       newKeys,
		Updated:    true,
	}, nil
}

func (node *BtreeNode) update() {
	node.Updated = true
	return
}

func (node *BtreeNode) overflow() bool {
	if node == nil {
		return false
	}
	return len(node.Keys) > node.Capacity
}

func (bt *Btree) Insert(key []byte, rid rid) error {
	if bt.root == nil {
		node, err := bt.newRootNode() //??????TODO:
		if err != nil {
			fmt.Println("new page for root node failed")
			return err
		}
		err = node.insertAt(key, rid, 0)
		if err != nil {
			fmt.Printf("insertAt failed:%v\n", err)
			return err
		}
		bt.root = node
		return nil
	}
	//fmt.Printf("key=%v\n", key)
	err := bt.root.insert(key, rid)
	if err != nil {
		fmt.Printf("insert failed:%v\n", err)
		return err
	}
	if bt.root.overflow() {
		//fmt.Println("Insert -> split()")
		return bt.root.split()
	}
	return nil
}

func (node *BtreeNode) insert(key []byte, rid rid) error {

	debug.Printf("+++ insert key: %s --> node keys:", key)
	printKeys(node.Keys)
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
				if err != nil {
					return err
				}
				node.update()
			} else {
				/*
					err := node.Pointers[i].node.insert(key, rid)
					if err != nil {
						return err
					}
					//fmt.Printf("split 2 node.Pointers[%v].Keys=%v\n", i, len(node.Pointers[i].Keys))
					if node.Pointers[i].node.overflow() {
						err := node.Pointers[i].node.split()
						if err != nil {
							return err
						}
					}
				*/
				child, err := node.Pointers[i].GetNode()
				if err != nil {
					return err
				}
				err = child.insert(key, rid)
				if err != nil {
					return err
				}
				if child.overflow() {
					err := child.split()
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
			//fmt.Printf("Key=%v\tk=%v\n", key, k)
			return DuplicateKeyError
		}
	}
	// right most pointer
	if node.Leaf {
		err := node.insertAt(key, rid, len(node.Keys))
		if err != nil {
			return err
		}
		node.update()
		if node.overflow() {
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
		child, err := node.Pointers[i].GetNode()
		if err != nil {
			return err
		}
		err = child.insert(key, rid)
		if err != nil {
			return err
		}
		if len(node.Pointers) > i && child.overflow() {
			err2 := child.split()
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
	debug.Printf("++++++ split keys:")
	printKeys(node.Keys)
	//fmt.Printf("split node.Pointers : %v\n", node.Pointers)

	center := node.Capacity / 2
	centerKey := node.Keys[center]
	//fmt.Printf("node.Capacity=%v\tlen(node.Keys)=%v centerKey=%s\n", node.Capacity, len(node.Keys), centerKey)

	parent, err := node.Parent.GetNode()
	if err != nil {
		return err
	}
	if parent == nil {
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
		left, err := node.newChildNode(node.Keys[:center])
		if err != nil {
			return err
		}
		right, err := node.newChildNode(node.Keys[center:])
		if err != nil {
			return err
		}
		//node.Capacity =   //TODO: change Capacity
		node.Keys = [][]byte{centerKey}
		if node.Leaf {
			node.Leaf = false
			left.Rids = node.Rids[:center]
			right.Rids = node.Rids[center:]
			node.Rids = []rid{}
			left.NextLeaf.SetNode(right)
			left.NextLeaf.SetPage(right.Page)
		} else {
			//
			//  P0 | Key0 | P1 | Key1 | P2 | Keys2 | P3
			//
			// ==>
			//                Key1
			//            /           \
			//  P0 | Key0 | P1      NIL | Key1 | P2 | Keys2 | P3
			//
			left.Pointers = make([]BtreeNodePtr, len(node.Pointers[:center+1]))
			copy(left.Pointers, node.Pointers[:center+1])
			for _, cPtr := range left.Pointers {
				child, err := cPtr.GetNode()
				if err != nil {
					fmt.Println("Get child pointer node")
					return err
				}
				child.Parent.node = left
			}

			right.Pointers = make([]BtreeNodePtr, len(node.Pointers[center:]))
			copy(right.Pointers, node.Pointers[center:])
			right.Pointers[0].SetNode(nil)
			for _, cPtr := range right.Pointers[1:] {
				child, err := cPtr.GetNode()
				if err != nil {
					fmt.Println("Get child pointer node")
					return err
				}
				child.Parent.SetNode(right)
			}
		}
		leftPtr := BtreeNodePtr{node: left, page: left.Page}
		rightPtr := BtreeNodePtr{node: right, page: right.Page}
		node.Pointers = []BtreeNodePtr{leftPtr, rightPtr}
		node.update()
		return nil
	}
	right, err := parent.newChildNode(node.Keys[center:])
	if err != nil {
		return err
	}
	newKeys := make([][]byte, len(node.Keys[:center]))
	copy(newKeys, node.Keys[:center])
	node.Keys = newKeys
	right.Leaf = node.Leaf
	if node.Leaf {
		// Rids
		newRids := make([]rid, len(node.Rids[center:]))
		copy(newRids, node.Rids[center:])
		right.Rids = newRids
		node.Rids = node.Rids[:center]

		// NextLeaf
		nextnode, err := node.NextLeaf.GetNode()
		if err != nil {
			return err
		}
		right.NextLeaf.SetNode(nextnode)
		if node.NextLeaf.node != nil {
			right.NextLeaf.SetPage(node.NextLeaf.GetPage())
		}
		node.NextLeaf.SetNode(right)
		node.NextLeaf.SetPage(right.Page)
	} else {
		right.Pointers = append([]BtreeNodePtr{BtreeNodePtr{node: nil}}, node.Pointers[center+1:]...)
		for _, cPtr := range right.Pointers {
			child, err := cPtr.GetNode()
			if err != nil {
				fmt.Println("split() get child pointer error")
				return err
			}
			if child != nil {
				child.Parent.SetNode(right)
				child.Parent.SetPage(right.Page)
			}
		}
		newPointers := make([]BtreeNodePtr, len(node.Pointers[:center+1]))
		copy(newPointers, node.Pointers[:center+1])
		node.Pointers = newPointers
		//fmt.Printf("After split left pointers(len:%v) right pointers(len:%v)\n", len(node.Pointers), len(right.Pointers))
	}
	err = parent.insertChildNodeByKey(right, centerKey)
	if err != nil {
		return err
	}
	/*
		if node.Parent.overflow() {
			return node.Parent.split()
		}
	*/
	node.update()
	return nil
}

func (node *BtreeNode) insertAt(key []byte, rid rid, index int) error {
	if index > len(node.Keys) {
		return errors.New("index out of range.")
	}
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
	//fmt.Printf("insertChildNodeByKey(child,%s) at index:%v\n", key, index)
	//fmt.Printf("node.Keys(len:%v): %s - %s\n", len(node.Keys), node.Keys[0], node.Keys[len(node.Keys)-1])
	//fmt.Printf("index=%v\n", index)
	//fmt.Printf("len(node.Pointers)=%v\n", len(node.Pointers))

	// insert key
	node.Keys = append(node.Keys, key) // extend one element
	copy(node.Keys[index+1:], node.Keys[index:])
	node.Keys[index] = key

	// insert child pointer
	ptrIndex := index + 1
	node.Pointers = append(node.Pointers, BtreeNodePtr{node: child, page: child.Page}) // extend one element
	copy(node.Pointers[ptrIndex+1:], node.Pointers[ptrIndex:])
	node.Pointers[ptrIndex] = BtreeNodePtr{node: child, page: child.Page}
	node.update()

	return nil
}

func (bt *Btree) Find(key []byte) (bool, *rid) {
	if bt.root == nil {
		return false, nil
	}
	return bt.root.find(key)
}

func (node *BtreeNode) find(key []byte) (bool, *rid) {
	for i, k := range node.Keys {
		switch result := bytes.Compare(key, k); {
		case result == 0 && node.Leaf:
			return true, &node.Rids[i]
		case result < 0 && node.Leaf:
			return false, nil
		case result <= 0:
			child, err := node.Pointers[i].GetNode()
			if err != nil {
				fmt.Println("GetNode() error")
				return false, nil
			}
			return child.find(key)
		}
	}
	if node.Leaf {
		return false, nil
	}
	child, err := node.Pointers[len(node.Keys)].GetNode()
	if err != nil {
		fmt.Println("GetNode() error")
		return false, nil
	}
	return child.find(key)
}

/*
func (bt *Btree) Delete(key []byte) error {
}

func (bt *Btree) SearchRange(key1, key2 []byte) error {
}
*/

func (bt *Btree) Commit() error {
	return bt.root.commit()
}

func (node *BtreeNode) commit() error {
	if node.Updated {
		// Write
	}
	for _, ptr := range node.Pointers {
		if ptr.node != nil {
			err := ptr.node.commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bt *Btree) PrintLeaves() {
	var err error
	node := bt.root
	for !node.Leaf {
		//node = node.Pointers[0].node
		node, err = node.Pointers[0].GetNode()
		if err != nil {
			fmt.Println("GetNode() error")
		}
	}
	count := 0
	var prev []byte
	for i := 0; ; i++ {
		for j, key := range node.Keys {
			if bytes.Compare(key, prev) < 0 {
				fmt.Printf("*** node:%v\tkey[%v]:%s prev:%s\n", i, j, key, prev)
			}
			fmt.Printf("node:%v\tkey[%v]:%s\n", i, j, key)
			prev = key
			count++
		}
		next, err := node.NextLeaf.GetNode()
		if err != nil {
			fmt.Println("GetNode() error")
		}
		//if node.NextLeaf.node == nil {
		if next == nil {
			break
		}
		//node = node.NextLeaf.node
		node = next
	}
	fmt.Printf("key count:%v\n", count)
}

func (bt *Btree) checkLeafKeyOrder() bool {
	node := bt.root
	for !node.Leaf {
		node = node.Pointers[0].node
	}
	count := 0
	var prev []byte
	for i := 0; ; i++ {
		for _, key := range node.Keys {
			if bytes.Compare(key, prev) < 0 {
				return false
			}
			prev = key
			count++
		}
		nextnode, err := node.NextLeaf.GetNode()
		if err != nil {
			fmt.Printf("GetNode() error:%v\n", err)
			return false
		}
		if nextnode == nil {
			break
		}
		node = node.NextLeaf.node
	}
	return true
}

func (bt *Btree) walk() []*BtreeNode {
	return bt.root.walk()
}

func (node *BtreeNode) walk() []*BtreeNode {
	var nodes []*BtreeNode
	nodes = append(nodes, node)
	if len(node.Pointers) <= 1 {
		return nodes
	}
	for _, ptr := range node.Pointers {
		child, err := ptr.GetNode()
		if err != nil {
			fmt.Printf("ptr.GetNode() in walk(): error %v\n", err)
		}
		if child != nil {
			nodes = append(nodes, child.walk()...)
		}
	}

	return nodes
}
