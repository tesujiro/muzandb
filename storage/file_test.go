package storage

import (
	"fmt"
	"testing"
)

func TestFile(t *testing.T) {
	f, err := newFile("xxx.dbf", BlockSize*1024*10)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(f)
}
