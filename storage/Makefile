test:
	- rm ./data/*.gob ./data/*.dbf
	#go test -v .
	go test -v -run TestBtree

cover:
	- rm ./data/*.gob ./data/*.dbf
	go test -v . -coverpkg . -coverprofile=cover.out && go tool cover -html=cover.out -o cover.html

