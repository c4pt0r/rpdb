all: build

build:
	@mkdir -p bin; rm -rf testdb-rocksdb/*
	go build -o bin/rpdb ./cmd && ./bin/rpdb -c conf/config.toml -n 4 --create

clean:
	rm -rf bin/* testdb-rocksdb sync.pipe

gotest:
	go test -cover -v ./pkg/... ./cmd/...
