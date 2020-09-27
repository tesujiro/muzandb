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
		header[i] = 0xFF // No Parent Page Pointer
		i += 5
	}

	// Header: Leaf
	var ui16 uint16
	if node.Leaf {
		ui16 = ui16 | 1<<15
	}
	// Header: Capacity
	capa := (node.Capacity << 15) >> 15
	//fmt.Printf("node.Capacity = %v\n", node.Capacity)
	ui16 = ui16 | uint16(capa)
	//fmt.Printf("ui16 = %v\n", ui16)

	endian.PutUint16(header[i:], ui16)
	i += 2

	// Number of Keys
	number := uint16(len(node.Keys))
	endian.PutUint16(header[i:], number)
	i += 2
	//fmt.Printf("number of keys:%v\n", number)

	// Header: NextLeafNode
	if node.Leaf {
		if node.NextLeaf.page == nil {
			header[i] = 0xFF
		} else {
			header[i] = byte(node.NextLeaf.page.file.FID)
			endian.PutUint32(header[i+1:], node.NextLeaf.page.pagenum)
		}
		i += 5
	}

	pd := PageData(header)
	return &pd
}

func (btree *Btree) ToPageData(node *BtreeNode) (*PageData, error) {
	index := 0
	page := make([]byte, PageSize)

	// Header
	header := btree.ToPageDataHeader(node)
	//fmt.Printf("HEADER: %v\n", header)
	if len(*header) > pageHeaderBytes {
		return nil, fmt.Errorf("header size %v > PageHeaderBytes %v", len(*header), pageHeaderBytes)
	}
	for index = 0; index < pageHeaderBytes; index++ {
		page[index] = (*header)[index]
	}

	// Keys
	for _, key := range node.Keys {
		//fmt.Printf("Key[%d]: %v\n", i, key)
		for j, r := range key {
			page[index+j] = r
		}
		index += int(btree.keylen)
	}

	// Rid, Pointers
	if node.Leaf {
		// Rids
		for _, rid := range node.Rids {
			page[index] = byte(rid.file.FID)
			endian.PutUint32(page[index+1:], rid.pagenum)
			endian.PutUint16(page[index+5:], rid.slotnum)
			index += 1 + 4 + 2
		}
	} else {
		// Pointers: Child Page Pointers
		for _, ptr := range node.Pointers {
			if ptr.page == nil {
				page[index] = 0xFF
			} else {
				//fmt.Printf("Ptr[%d]: FID=%v pagenum=%v\n", i, ptr.page.file.FID, ptr.page.pagenum)
				page[index] = byte(ptr.page.file.FID)
				endian.PutUint32(page[index+1:], ptr.page.pagenum)
			}
			index += 1 + 4
		}
	}
	//fmt.Printf("PAGE: %v\n", page)
	pd := PageData(page)
	return &pd, nil
}

func (btree *Btree) ToNode(pd *PageData) (*BtreeNode, error) {

	node := &BtreeNode{Tablespace: btree.tablespace}
	data := []byte(*pd)
	index := 0
	//fmt.Printf("data=%v\n", data)

	// Header: Page Type
	pageType := PageType(data[index])
	index += 1
	if pageType != BtreeLeafPage && pageType != BtreeNonLeafPage {
		return nil, errors.New("Not a BtreeNode data")
	}
	//fmt.Printf("pageType=%T\n", pageType)

	getPage := func(data []byte, i int) (*Page, error) {
		if data[i] == 0xFF {
			return nil, nil
		}
		page := &Page{}
		fid := FID(data[i])
		file, err := btree.tablespace.getFile(fid)
		if err != nil {
			return nil, errors.New("No FID in Tablespace")
		}
		page.file = file
		i += 1
		page.pagenum = endian.Uint32(data[i:])

		return page, nil
	}

	// Header: Page Pointer
	page, err := getPage(data, index)
	if err != nil {
		return nil, err
	}
	index += 5
	node.Page = page

	// Header: Parent Page Pointer
	page, err = getPage(data, index)
	if err != nil {
		return nil, err
	}
	index += 5
	node.Parent.page = page

	// Header: Leaf
	leaf_cap := endian.Uint16(data[index : index+2])
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
	index += 2
	//fmt.Printf("node.Capacity = %v\n", node.Capacity)

	// Header: Number Of Keys
	numberOfKeys := int(endian.Uint16(data[index : index+2]))
	index += 2
	_ = numberOfKeys
	//fmt.Printf("numberOfKeys = %v\n", numberOfKeys)

	// Header: NextLeafNode
	if node.Leaf {
		page, err = getPage(data, index)
		if err != nil {
			return nil, err
		}
		index += 5
		node.NextLeaf.page = page
	}

	index = pageHeaderBytes

	//Keys
	node.Keys = make([][]byte, numberOfKeys)
	for i := 0; i < numberOfKeys; i++ {
		node.Keys[i] = make([]byte, int(btree.keylen))
		copy(node.Keys[i], data[index:index+int(btree.keylen)])
		index += int(btree.keylen)
	}

	// Rid, Pointers
	if node.Leaf {
		// Rids
		node.Rids = make([]rid, numberOfKeys)
		for i := 0; i < numberOfKeys; i++ {
			fid := FID(data[index])
			file, err := btree.tablespace.getFile(fid)
			if err != nil {
				return nil, err
			}
			//fmt.Printf("getFile(%v)=%v\n", fid, *file)
			node.Rids[i].file = file
			node.Rids[i].pagenum = endian.Uint32(data[index+1:])
			node.Rids[i].slotnum = endian.Uint16(data[index+5:])
			index += 1 + 4 + 2
		}

	} else {
		// Pointers: Child Page Pointers
		ptrs := make([]BtreeNodePtr, numberOfKeys+1)
		node.Pointers = make([]BtreeNodePtr, numberOfKeys+1)
		for i := 0; i < numberOfKeys+1; i++ {
			page, err = getPage(data, index)
			if err != nil {
				return nil, err
			}
			index += 5
			ptrs[i].page = page
			node.Pointers[i] = ptrs[i]
		}
	}
	node.Updated = false

	return node, nil
}
