package storage

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
)

const PageSize = 1024
const dataPath = "./data"
const pageMangerMetaPath = dataPath + "/page_manager.gob"

type PageManager struct {
	Tablespaces []*Tablespace
	LastFID     FID
}

type FID uint8

func (pm *PageManager) newFile(path string, size uint32) *File {
	file := newFile(pm.LastFID, path, size)
	pm.LastFID++
	return file
}

func startPageManager() *PageManager {
	fp, err := os.Open(pageMangerMetaPath)
	if err != nil {
		fmt.Println("Create New PageManager")
		return &PageManager{}
	}
	defer fp.Close()

	var pm *PageManager
	dec := gob.NewDecoder(fp)
	if err := dec.Decode(&pm); err != nil {
		log.Fatal("decode error:", err)
	}
	for _, ts := range pm.Tablespaces {
		for _, file := range ts.File {
			err := file.open()
			if err != nil {
				log.Fatal("file open error:", err)
			}
		}
	}
	return pm
}

func (pm *PageManager) Save() error {
	fp, err := os.Create(pageMangerMetaPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer fp.Close()
	enc := gob.NewEncoder(fp)

	if err := enc.Encode(*pm); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (pm *PageManager) Stop() error {
	for _, ts := range pm.Tablespaces {
		for _, file := range ts.File {
			if file.fp != nil {
				file.fp.Close()
			}
		}
	}
	return nil
}

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

func (pm *PageManager) newTablespace(name string) (*Tablespace, error) {
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
	err := file.create()
	if err != nil {
		return err
	}
	ts.File = append(ts.File, file)
	return nil
}

func (ts *Tablespace) newPage() (*Page, error) {
	pagenum := uint32(1 << 31)
	var target *File
	for _, file := range ts.File {
		if file.CurPage < pagenum && file.CurPage+1 < file.Pages {
			target = file
			pagenum = file.Pages
		}
	}
	page, err := target.newPage()
	if err != nil {
		return nil, err
	}

	return page, nil
}
