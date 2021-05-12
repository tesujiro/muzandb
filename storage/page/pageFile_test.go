package page

import (
	"testing"
)

const dataPath = "./data"

func TestPageFile(t *testing.T) {
	var err error
	/*
		f := &fio.File{Path: dataPath + "/testOpenClose.dbf"}
		defer f.Close()

		err = f.Create()
		if err != nil {
			t.Fatalf("File.Create error = %v\n", err)
		}
	*/

	FID := FID(0)
	filePath := dataPath + "/TestPageFile.dbf"
	pages := uint32(10)

	pf := NewPageFile(FID, filePath, pages*PageSize)
	err = pf.Create()
	if err != nil {
		t.Fatalf("PageFile.Create error = %v\n", err)
	}

	data := []byte("12345678990ABCDEF")
	p := newPage(pf, uint32(1), data)
	err = p.write()
	if err != nil {
		t.Fatalf("PageFile.writePage error = %v\n", err)
	}

}
