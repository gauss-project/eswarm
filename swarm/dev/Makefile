.PHONY: build cluster test

default: build

build:
	go build -o bin/swarm    github.com/gauss-project/eswarm/cmd/swarm
	go build -o bin/geth     github.com/gauss-project/eswarm/cmd/geth
	go build -o bin/bootnode github.com/gauss-project/eswarm/cmd/bootnode

cluster: build
	scripts/boot-cluster.sh

test:
	go test -v github.com/gauss-project/eswarm/swarm/...
