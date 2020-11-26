package storage

import (
	"fmt"
	"os"
)

type Tablespace struct {
	Name string
	File []*File
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

func (ts *Tablespace) addFile(file *File) error {
	err := file.open()
	if os.IsNotExist(err) {
		err := file.create()
		if err != nil {
			return err
		}
	} else {
		return err
	}
	ts.File = append(ts.File, file)
	return nil
}

// TODO: Tablespace.getFile -> GetFile
func (ts *Tablespace) getFile(fid FID) (*File, error) {
	return GetFile(fid)
}

func (ts *Tablespace) NewPage() (*Page, error) {
	// Roundrobin
	// TODO: least used / all
	pagenum := uint32(1 << 31)
	var target *File
	if len(ts.File) == 0 {
		return nil, fmt.Errorf("No file in tablespace: %v\n", ts)
	}
	for _, file := range ts.File {
		if file.CurPage < pagenum && file.CurPage+1 < file.Pages {
			target = file
			pagenum = file.CurPage
		}
	}
	return target.newPage()
}
