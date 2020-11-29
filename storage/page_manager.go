package storage

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
)

//const PageSize = 512
const PageSize = 1024
const dataPath = "./data"
const pageMangerMetaPath = dataPath + "/page_manager.gob"

type FID uint8

var FileMap map[FID]*File = make(map[FID]*File)

type PageManager struct {
	Tablespaces []*Tablespace
	LastFID     FID
	//FileMap     map[FID]*File
	//Files       map[FID]*File
}

func (pm *PageManager) NewFile(path string, size uint32) *File {
	fid := pm.LastFID
	file := newFile(pm.LastFID, path, size)
	FileMap[fid] = file
	pm.LastFID++
	return file
}

func GetFile(fid FID) (*File, error) {
	file := FileMap[fid]
	if file == nil {
		fmt.Printf("FID(%v) not found.\n", fid)
		return nil, NoKeyError
	}
	return file, nil
}

func startPageManager() *PageManager {
	fp, err := os.Open(pageMangerMetaPath)
	if err != nil {
		//fmt.Println("Create New PageManager")
		return &PageManager{
			//FileMap: make(map[FID]*File),
		}
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
