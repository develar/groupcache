#! /bin/sh

# Make sure the script fails fast.
set -eux

protoc --go_out=. examplepb/example.proto --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+pool
protoc --go_out=. testpb/test.proto --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+pool
protoc --go_out=. --go-vtproto_out=. --go-vtproto_opt=features=marshal+unmarshal+size+pool value.proto

# protoc --go_out=. --go-vtproto_out=. --go-drpc_out=. --go-drpc_opt=protolib=github.com/planetscale/vtprotobuf/codec/drpc
