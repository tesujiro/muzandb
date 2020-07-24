package storage

import (
	"fmt"
	"testing"
)

func TestFile(t *testing.T) {
	f := newFile(1, "xxx.dbf", PageSize*10)
	defer f.close()

	f.create()

	err := f.write(2, 10, []byte("0123456789"))
	if err != nil {
		fmt.Println(err)
	}
	buf, err := f.read(2, 10, 10)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("read()=%v\n", buf)
	}

	buf, err = f.readBlock(2)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("readBlock()=%v\n", buf)
	}

}
