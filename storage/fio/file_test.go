package fio

import (
	"bytes"
	"testing"
)

const dataPath = "./data"

func TestFileOpen(t *testing.T) {
	f := &File{Path: dataPath + "/testOpenClose.dbf"}
	defer f.close()

	var err error
	err = f.Create()
	if err != nil {
		t.Fatalf("File.Create error = %v\n", err)
	}

	err = f.close()
	if err != nil {
		t.Fatalf("File.close error = %v\n", err)
	}

	err = f.open()
	if err != nil {
		t.Fatalf("File.open error = %v\n", err)
	}

}

func TestFileIO(t *testing.T) {
	f := &File{Path: dataPath + "/testReadWrite.dbf"}
	defer f.close()

	err := f.Create()
	if err != nil {
		t.Fatalf("File.Create error = %v\n", err)
	}

	tests := []struct {
		offset int64
		data   []byte
	}{
		{offset: 0, data: []byte("0123456789")},
		{offset: 2*1024 + 10, data: []byte("0123456789")},
	}

	for _, test := range tests {
		err = f.Write(test.offset, test.data)
		if err != nil {
			//fmt.Println(err)
			t.Fatalf("File.Write error = %v\n", err)
		}
		got, err := f.Read(test.offset, len(test.data))
		if err != nil {
			t.Fatalf("File.Read error = %v\n", err)
		} else if bytes.Compare(got, test.data) != 0 {
			t.Errorf("File.Read want:%v got:%v\n", got, test.data)
		}
	}
}
