package storage

import (
	"fmt"
	"os"

	"github.com/tesujiro/muzandb/storage/page"
)

type Tablespace struct {
	Name string
	File []*page.PageFile
}

func (ts *Tablespace) String() string {
	var str string
	if len(ts.File) > 0 {
		str = fmt.Sprintf("%v", ts.File[0])
		for _, fi := range ts.File[1:] {
			str = fmt.Sprintf("%v,%v", str, fi)
		}
	}
	return fmt.Sprintf("{%v [%v]}", ts.Name, str)
}

func (pm *PageManager) NewTablespace(name string) (*Tablespace, error) {
	//TODO: existance check
	for _, ts := range pm.Tablespaces {
		if ts.Name == name {
			return nil, fmt.Errorf("Tablespace already exists: %v", name)
		}
	}

	ts := Tablespace{Name: name}
	pm.Tablespaces = append(pm.Tablespaces, &ts)
	return &ts, nil
}

func (ts *Tablespace) addFile(file *page.PageFile) error {
	err := file.Open()
	if os.IsNotExist(err) {
		err := file.Create()
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	ts.File = append(ts.File, file)
	return nil
}

// TODO: Tablespace.getFile -> GetFile
/*
func (ts *Tablespace) getFile(fid FID) (*File, error) {
	return GetFile(fid)
}
*/

func (ts *Tablespace) NewPage() (*page.Page, error) {
	// Roundrobin
	// TODO: least used / all
	pagenum := uint32(1 << 31)
	var target *page.PageFile
	if len(ts.File) == 0 {
		return nil, fmt.Errorf("No file in tablespace: %v\n", ts)
	}
	for _, file := range ts.File {
		if file.CurPage < pagenum && file.CurPage+1 < file.Pages {
			target = file
			pagenum = file.CurPage
		}
	}
	if target == nil {
		return nil, fmt.Errorf("No space in tablespace: %v", ts)
	}
	return target.NewPage()
}
