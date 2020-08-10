package storage

//func (node *BtreeNode) ToPageData() (*PageData, error) {
func (btree *Btree) ToPageData(node *BtreeNode) (*PageData, error) {
	// Header: Parent Page Pointer
	// Header: Leaf
	// Header: Capacity

	if node.Leaf {
		// Keys
		// Rids
		// Header: NextLeafNode

	} else {
		// Keys
		// Pointers: Child Page Pointers

	}
	return nil, nil
}

func (btree *Btree) ToNode(data *PageData) (*BtreeNode, error) {
	return nil, nil
}
