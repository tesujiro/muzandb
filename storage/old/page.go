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
	Pd_lsn             PageXLogRecPtr
	Pd_checksum        uint16
	Pd_flags           uint16
	Pd_lower           LocationIndex
	Pd_upper           LocationIndex
	Pd_pagesize_verion uint16
	Pd_prune_xid       TransactionId
}

type LocationIndex uint16

type PageXLogRecPtr struct {
	Xlogid  uint32
	Xrecoff uint32
}

type TransactionId uint32
