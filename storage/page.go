package storage

type Page struct {
	PageHeader   *PageHeader
	ItemIdData   []*ItemIdData
	FreeSpace    []byte
	Item         []*Item
	SpecialSpace []byte //TODO; struct
}

type ItemIdData [4]byte

type Item []byte

type PageHeader struct {
	pd_lsn             PageXLogRecPtr
	pd_checksum        uint16
	pd_flags           uint16
	pd_lower           LocationIndex
	pd_upper           LocationIndex
	pd_pagesize_verion uint16
	pd_prune_xid       TransactionId
}

type LocationIndex uint16

type PageXLogRecPtr struct {
	xlogid  uint32
	xrecoff uint32
}

type TransactionId uint32
