# intkey-stress-test

Simple utility for stress testing Hyperledger Sawtooth with `intkey` transactions.

## Building
To build a Docker container containing the test, do the following

    git clone https://github.com/taekion-org/intkey-stress-test.git
    cd intkey-stress-test
    docker build -t taekion/intkey-stress-test .

## Running
To run the test utility (and display help):

    docker run -t taekion/intkey-stress-test --help
