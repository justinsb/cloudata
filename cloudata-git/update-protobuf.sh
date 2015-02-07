#!/bin/bash

protoc src/main/proto/*.proto --proto_path . --proto_path /usr/include --proto_path /usr/local/include --java_out src/main/java
