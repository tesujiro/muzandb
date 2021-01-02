package transaction

import (
	"testing"
)

func TestTxid(t *testing.T) {
	for i := 0; i < 100; i++ {
		tid := newTxid()
		//fmt.Printf("newTxid()=%v\n", tid)
		want := Txid(i + 1)
		if tid != want {
			t.Fatalf("Txid: want:%v got:%v", want, tid)
		}
	}
	_txid = _maxTxid
	tid := newTxid()
	want := Txid(1)
	if tid != want {
		t.Fatalf("Txid: want:%v got:%v", want, tid)
	}

}
