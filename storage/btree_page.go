package storage

func (btree *Btree) ToPageDataHeader(node *BtreeNode) (*PageData, error) {
	// Header: Page Type
	// Header: Parent Page Pointer
	// Header: Leaf
	// Header: Capacity
	if node.Leaf {
		// Header: NextLeafNode
	}

	return nil, nil
}

//func (node *BtreeNode) ToPageData() (*PageData, error) {
func (btree *Btree) ToPageData(node *BtreeNode) (*PageData, error) {
	if node.Leaf {
		// Keys
		// Rids
	} else {
		// Keys
		// Pointers: Child Page Pointers
	}
	return nil, nil
}

func (btree *Btree) ToNode(data *PageData) (*BtreeNode, error) {
	return nil, nil
}
