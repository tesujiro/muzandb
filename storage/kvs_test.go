package storage

import (
	"bytes"
	"testing"
)

func TestKvs(t *testing.T) {
	db, err := OpenFile("./testdata")
	if err != nil {
		t.Errorf("Open failed: %v", err)
	}
	defer db.Close()

	key := []byte("key")
	val := []byte("value")

	err = db.Put(key, val)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}

	ret, err := db.Get(key)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if bytes.Compare(ret, val) != 0 {
		t.Errorf("want: %s get : %s", val, ret)
	}

}
