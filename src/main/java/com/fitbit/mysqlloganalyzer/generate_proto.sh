#!/usr/bin/env bash
protoc --java_out=./../../../ -I ./ ./*proto
