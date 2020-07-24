package storage

import (
	"fmt"
	"testing"
)

func TestPageManager(t *testing.T) {
	var err error

	pm := startPageManager()

	file1 := pm.newFile("./data/file1.dbf", 1024*1024)
	file2 := pm.newFile("./data/file2.dbf", 1024*1024)
	file3 := pm.newFile("./data/file3.dbf", 1024*1024)

	ts1, err := pm.newTablespace("TABLESPACE1")
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}

	err = ts1.addFile(file1)
	if err != nil {
		fmt.Printf("addFile(%v) error: %v\n", file1, err)
	}
	err = ts1.addFile(file2)
	if err != nil {
		fmt.Printf("addFile(%v) error: %v\n", file2, err)
	}
	err = ts1.addFile(file3)
	if err != nil {
		fmt.Printf("addFile(%v) error: %v\n", file3, err)
	}
	fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

	err = pm.Save()
	if err != nil {
		fmt.Printf("save error: %v\n", err)
	}

	err = pm.Stop()
	if err != nil {
		fmt.Printf("stop error: %v\n", err)
	}
	pm = startPageManager()
	fmt.Printf("pm.Tablespaces: %v\n", pm.Tablespaces)

	//fp1 := pm.Tablespaces[0].F_info[0].fp
	//fp1.readBlock(0)
}
