#!/bin/bash

for I in *; do
	if [[ -e ${I}/update-protobuf.sh ]]; then
		pushd ${I}
		./update-protobuf.sh
		popd
	fi
done
