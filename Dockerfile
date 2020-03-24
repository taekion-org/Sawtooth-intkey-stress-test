FROM hyperledger/sawtooth-shell:1.1.5

RUN apt-get update -y
RUN apt-get install -y \
    wget \
	git \
    libssl-dev \
    libzmq3-dev \
    openssl \
    protobuf-compiler \
    python3 \
    python3-pip \
    pkg-config

RUN python3 -m pip install grpcio grpcio-tools

RUN wget https://dl.google.com/go/go1.13.9.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.13.9.linux-amd64.tar.gz
RUN mkdir /go
ENV GOPATH=/go
RUN echo $PATH
ENV PATH=$PATH:/usr/local/go/bin:/go/bin

RUN go get \
    github.com/golang/protobuf/proto \
    github.com/golang/protobuf/protoc-gen-go \
    github.com/golang/mock/gomock \
    github.com/golang/mock/mockgen \
    github.com/pebbe/zmq4

RUN go get github.com/hyperledger/sawtooth-sdk-go
RUN bash -c "cd /go/src/github.com/hyperledger/sawtooth-sdk-go && git checkout v0.1.3"
WORKDIR /go/src/github.com/hyperledger/sawtooth-sdk-go/
RUN go generate

ADD . /go/src/github.com/taekion-org/intkey-stress-test
WORKDIR /go/src/github.com/taekion-org/intkey-stress-test
RUN go get ./...
RUN go build

RUN sawtooth keygen

FROM ubuntu:18.04
RUN apt-get update
RUN apt-get -y install libssl-dev libzmq3-dev openssl libssl1.0.0
COPY --from=0 /go/src/github.com/taekion-org/intkey-stress-test/intkey-stress-test /intkey-stress-test
COPY --from=0 /root/.sawtooth /root/.sawtooth

ENTRYPOINT ["/intkey-stress-test"]
