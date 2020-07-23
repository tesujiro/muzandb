package storage

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
)

const BlockSize = 1024
const dataPath = "./data"
const pageMangerMetaPath = dataPath + "/page_manager.gob"

type PageManager struct {
	Tablespaces []*Tablespace
	LastFID     FID
}

type FID uint8

type FileInfo struct {
	FID  FID
	Path string
	Size uint32
	fd   *os.File
}

func (pm *PageManager) newFileInfo(path string, size uint32) FileInfo {
	//TODO: existence check

	pm.LastFID++
	return FileInfo{FID: pm.LastFID, Path: path, Size: size}
}

func startPageManager() *PageManager {
	f, err := os.Open(pageMangerMetaPath)
	if err != nil {
		fmt.Println("Create New PageManager")
		return &PageManager{}
	}
	defer f.Close()

	var pm *PageManager
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&pm); err != nil {
		log.Fatal("decode error:", err)
	}
	for _, ts := range pm.Tablespaces {
		for _, fi := range ts.F_info {
			f, err := getFile(fi.Path)
			if err != nil {
				log.Fatal("file open error:", err)
			}
			fi.fd = f.fp
		}
	}

	return pm
}

func (pm *PageManager) Save() error {
	f, err := os.Create(pageMangerMetaPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)

	if err := enc.Encode(*pm); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (pm *PageManager) Stop() error {
	for _, ts := range pm.Tablespaces {
		for _, fi := range ts.F_info {
			if fi.fd != nil {
				fi.fd.Close()
			}
		}
	}
	return nil
}

type Tablespace struct {
	Name   string
	F_info []*FileInfo
}

func (ts *Tablespace) String() string {
	var str string
	if len(ts.F_info) > 0 {
		str = fmt.Sprintf("%v", ts.F_info[0])
		for _, fi := range ts.F_info[1:] {
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

func (ts *Tablespace) addFile(fi FileInfo) error {
	fd, err := newFile(fi.Path, fi.Size)
	if err != nil {
		return err
	}
	fi.fd = fd.fp
	ts.F_info = append(ts.F_info, &fi)
	return nil
}

/*
func (ts *Tablespace) newPhysicalPage() error {
	return nil
}
*/
