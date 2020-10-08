test:
	- rm ./data/*.gob ./data/*.dbf
	go test -v .

cover:
	- rm ./data/*.gob ./data/*.dbf
	go test -v . -coverpkg . -coverprofile=cover.out && go tool cover -html=cover.out -o cover.html

