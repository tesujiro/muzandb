test:
	- mkdir ./data ./fio/data ./page/data
	- rm ./data/*.gob ./data/*.dbf ./fio/data/* ./page/data/* 2>/dev/null
	go test -v github.com/tesujiro/muzandb/storage/fio github.com/tesujiro/muzandb/storage/page .

cover:
	- mkdir ./data ./fio/data ./page/data
	- rm ./data/*.gob ./data/*.dbf ./fio/data/* ./page/data/* 2>/dev/null
	go test -v . -coverpkg . -coverprofile=cover.out && go tool cover -html=cover.out -o cover.html

