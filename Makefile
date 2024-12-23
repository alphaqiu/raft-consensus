.PHONY: build
build: clean
	go build -o demo-cluster cmd/demo-cluster/*.go

clean:
	@rm -f demo-cluster
