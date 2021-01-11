#!/bin/bash

port=$1


echo "[EGGROLL] starting iperf3 server on port=${port}"
echo "================================================"
iperf3 -s -p ${port}
