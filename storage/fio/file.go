package fio

import (
	"fmt"
	"os"
)

type File struct {
	Path string
	fp   *os.File
}

func (file *File) Create() error {
	if _, err := os.Stat(file.Path); err == nil {
		return fmt.Errorf("file %s already exists.", file.Path)
	} else if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	fp, err := os.Create(file.Path)
	if err != nil {
		fmt.Println(err)
		return err
	}
	file.fp = fp

	return nil
}

func (file *File) open() error {
	_, err := os.Stat(file.Path)
	if err != nil {
		return err
	}
	//fp, err := os.Open(file.Path)
	fp, err := os.OpenFile(file.Path, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	file.fp = fp

	return nil
}

func (file *File) Write(offset int64, buf []byte) error {
	file.fp.Seek(offset, os.SEEK_SET)
	_, err := file.fp.Write(buf)
	return err
}

func (file *File) Read(offset int64, size int) ([]byte, error) {

	file.fp.Seek(offset, os.SEEK_SET)
	buf := make([]byte, size)
	_, err := file.fp.Read(buf)
	if err != nil {
		fmt.Printf("Error in file.read: %T %v size: %v buf:%v\n", err, err, size, buf)
	}
	//fmt.Printf("Normal in file.read: page: %v byt: %v size: %v \n", page, byt, size)
	return buf, err
}

func (file *File) close() error {
	return file.fp.Close()
}
