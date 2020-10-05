package storage

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestSlottedPage(t *testing.T) {
	pm := startPageManager()
	datafile1 := pm.NewFile("./data/TestSlottedPage_datafile1.dbf", 1024*1024)

	ts_dat, err := pm.NewTablespace("DATA TABLESPACE 1")
	if err != nil {
		t.Fatalf("PageManger.newTablespace() error:%v", err)
	}

	err = ts_dat.addFile(datafile1)
	if err != nil {
		t.Errorf("Tablespace.addFile(%v) error:%v", datafile1, err)
	}

	tc := []struct {
		data []string
		err  error
	}{
		{[]string{"test data"}, nil},
		//{[]string{"test data", "TEST DATA"}, nil},
		{[]string{"test data", "TEST DATA", "TEST DATA3"}, nil},
		{[]string{fmt.Sprintf("%.4096d", 0)}, NoSpaceError},
	}

	for testNumber, test := range tc {
		fmt.Printf("Testcase[%v]: %v\n", testNumber, test)
		sp, err := newSlottedPage(ts_dat)
		if err != nil {
			t.Errorf("Testcase[%v]: newSlottedPage() error:%v", testNumber, err)
		}

		for _, data := range test.data {
			rid, err := sp.Insert([]byte(data))
			if err != test.err {
				t.Errorf("Testcase[%v]: SlottedPage.InsertData(%s) error:%v", testNumber, data, err)
			}
			//t.Logf("rid:%v", rid)
			_ = rid
		}
		if test.err == nil {
			original := sp
			pd, err := original.ToPageData()
			if err != nil {
				t.Errorf("Testcase[%v]: SlottedPage.ToPageData() error:%v", testNumber, err)
			}

			restored, err := pd.ToSlottedPage(original.pctfree)
			if err != nil {
				t.Errorf("Testcase[%v]: PageData.ToSlottedPage() error:%v", testNumber, err)
			}
			if restored.String() != original.String() {
				t.Errorf("Testcase[%v]: Restored node != Original node", testNumber)
				t.Errorf("Original Slotted Page: %v\n", original)
				t.Errorf("Restored Slotted Page: %v\n", restored)
				t.Errorf("data: %v\n", pd)
				t.Errorf("data:\n%s", hex.Dump([]byte(*pd)))
			}
		}
	}
}