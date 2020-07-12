package storage

import (
	"fmt"
	"testing"
)

func TestFile(t *testing.T) {
	f, err := newFile("xxx.dbf", BlockSize*10)
	if err != nil {
		fmt.Println(err)
	}
	err = f.write(2, 10, []byte("0123456789"))
	if err != nil {
		fmt.Println(err)
	}
}
