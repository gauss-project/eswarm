#!/usr/bin/env bash

docker run -p 80:80 -v /data:/data -d nginx:latest