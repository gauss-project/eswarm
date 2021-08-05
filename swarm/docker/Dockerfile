FROM ubuntu:latest as golang

RUN apt-get update
RUN apt-get install -y wget git gcc apt-utils

RUN wget -P /tmp https://dl.google.com/go/go1.11.5.linux-amd64.tar.gz

RUN tar -C /usr/local -xzf /tmp/go1.11.5.linux-amd64.tar.gz
RUN rm /tmp/go1.11.5.linux-amd64.tar.gz

ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH"

# WORKDIR $GOPATH

#FROM golang:1.12.5-alpine as builder

ARG VERSION

RUN apt-get -y install git gcc g++   autoconf automake libtool make govendor



RUN  mkdir -p $GOPATH/src/github.com/plotozhu/ && \
    cd $GOPATH/src/github.com/plotozhu && \
    git clone https://3823a1162e4a52f6e284610be5151c4204a900e7:@github.com/gauss-project/eswarm && \
    cd $GOPATH/src/github.com/gauss-project/eswarm && \
    git checkout ${VERSION}    && cd $GOPATH/src/github.com/gauss-project/eswarm &&\
    mkdir -p vendor && mv vendor_bak.json vendor/vendor.json

RUN     cd $GOPATH/src/github.com/gauss-project/eswarm &&\
         govendor sync

RUN     cd $GOPATH/src/github.com/gauss-project/eswarm/vendor/github.com/plotozhu/wiredtiger-go && \
        git pull  && sh install-deps.sh



RUN    cd  $GOPATH/src/github.com/gauss-project/eswarm && \
    go install -ldflags "-X main.gitCommit=${VERSION}" ./cmd/swarm && \
    go install -ldflags "-X main.gitCommit=${VERSION}" ./cmd/swarm/swarm-smoke && \
    go install -ldflags "-X main.gitCommit=${VERSION}" ./cmd/swarm/global-store && \
    go install -ldflags "-X main.gitCommit=${VERSION}" ./cmd/geth


FROM ubuntu:latest as esc

WORKDIR /
COPY --from=golang /go/bin/swarm /
COPY --from=golang /go/bin/geth /
COPY --from=golang /go/bin/swarm-smoke /
COPY --from=golang /go/bin/global-store /

ENTRYPOINT ["/config/run.sh"]
