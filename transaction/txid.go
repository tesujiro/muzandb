package transaction

type Txid uint64

const _maxTxid = Txid(^uint64(0))

var _txid Txid

func newTxid() Txid {
	if _txid == _maxTxid {
		_txid = 0
	}
	_txid++
	return _txid
}

func saveTxid() {
}

func loadTxid() {
}

//func CompareTxid(a, b txid) int {
//}
