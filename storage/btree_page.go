package storage

import (
	"errors"
	"fmt"
)

func (btree *Btree) ToPageDataHeader(node *BtreeNode) *PageData {
	header := make([]byte, pageHeaderBytes)
	i := 0
	// Header: Page Type
	if node.Leaf {
		header[i] = byte(BtreeLeafPage)
	} else {
		header[i] = byte(BtreeNonLeafPage)
	}
	i += 1

	// Header: Page Pointer
	header[i] = byte(node.Page.file.FID)
	i += 1
	endian.PutUint32(header[i:], node.Page.pagenum)
	i += 4

	// Header: Parent Page Pointer
	if node.Parent.page != nil {
		header[i] = byte(node.Parent.page.file.FID)
		i += 1
		endian.PutUint32(header[i:], node.Parent.page.pagenum)
		i += 4
	} else {
		i += 5
	}

	// Header: Leaf
	var ui16 uint16
	if node.Leaf {
		ui16 = ui16 | 1<<15
	}
	// Header: Capacity
	capa := (node.Capacity << 15) >> 15
	ui16 = ui16 | uint16(capa)

	endian.PutUint16(header[i:], ui16)
	i += 2

	// Header: NextLeafNode
	if node.Leaf {
		header[i] = byte(node.NextLeaf.page.file.FID)
		i += 1
		endian.PutUint32(header[i:], node.NextLeaf.page.pagenum)
		i += 4
	}

	pd := PageData(header)
	return &pd
}

//func (node *BtreeNode) ToPageData() (*PageData, error) {
func (btree *Btree) ToPageData(node *BtreeNode) (*PageData, error) {
	header := btree.ToPageDataHeader(node)
	fmt.Printf("HEADER: %v\n", header)
	if len(*header) > pageHeaderBytes {
		return nil, fmt.Errorf("header size %v > PageHeaderBytes %v", len(*header), pageHeaderBytes)
	}

	index := pageHeaderBytes - 1

	// Keys
	for i, key := range node.Keys {
		fmt.Printf("Key[%d]: %v\n", i, key)
	}
	if node.Leaf {
		// Rids
	} else {
		// Pointers: Child Page Pointers
		for i, ptr := range node.Pointers {
			fmt.Printf("Ptr[%d]: FID=%v pagenum=%v\n", i, ptr.page.file.FID, ptr.page.pagenum)
		}
	}
	return nil, nil
}

func (btree *Btree) ToNode(pd *PageData) (*BtreeNode, error) {
	node := &BtreeNode{}
	data := []byte(*pd)
	i := 0

	// Header: Page Type
	pageType := PageType(data[i])
	i += 1
	if pageType != BtreeLeafPage && pageType != BtreeNonLeafPage {
		return nil, errors.New("Not a BtreeNode data")
	}

	page := &Page{}
	node.Page = page
	// Header: Parent Page Pointer
	node.Page.file.FID = FID(uint8(data[i]))
	i += 1
	node.Page.pagenum = endian.Uint32(data[0:])
	i += 4

	// Header: Leaf
	leaf_cap := endian.Uint16(data[i : i+2])
	leaf := (leaf_cap >> 15) == 1
	switch {
	case leaf && pageType == BtreeNonLeafPage:
		return nil, errors.New("BtreeNonLeafPage but leaf==true")
	case !leaf && pageType == BtreeLeafPage:
		return nil, errors.New("BtreeLeafPage but leaf==false")
	default:
		node.Leaf = leaf
	}

	// Header: Capacity
	node.Capacity = int((leaf_cap << 1) >> 1)
	i += 2

	// Header: NextLeafNode
	if node.Leaf {
		nextPage := &Page{}
		node.NextLeaf.page = nextPage

		node.NextLeaf.page.file.FID = FID(uint8(data[i]))
		i += 1
		node.NextLeaf.page.pagenum = endian.Uint32(data[i:])
		i += 4
	}

	return nil, nil
}
