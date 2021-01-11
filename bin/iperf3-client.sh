#!/bin/bash


ip=$1
port=$2
bandwidth=$3

echo "[EGGROLL] starting iperf3 client to ip=${ip}, port=${port}, bandwidth=${bandwidth}"
echo "================================================"


iperf3 -c ${ip} -p ${port} -b ${bandwidth} -i 1 -P 1 -t 10 -T "${bandwidth}/parallel=1/TCP/C->S"
echo "[EGGROLL] test finished (1/8): ${bandwidth}/parallel=1/TCP/C->S"
echo "================================================"
echo

iperf3 -c ${ip} -p ${port} -b ${bandwidth} -i 1 -P 1 -t 10 -R -T "${bandwidth}/parallel=1/TCP/S->C"
echo "[EGGROLL] test finished (2/8): ${bandwidth}/parallel=1/TCP/S->C"
echo "================================================"
echo

iperf3 -c ${ip} -p ${port} -b 1m -i 1 -P 1 -t 10 -T "1m/parallel=1/TCP/C->S"
echo "[EGGROLL] test finished (3/8): 1m/parallel=1/TCP/C->S"
echo "================================================"
echo

iperf3 -c ${ip} -p ${port} -b 1m -i 1 -P 1 -t 10 -R -T "1m/parallel=1/TCP/S->C"
echo "[EGGROLL] test finished (4/8): 1m/parallel=1/TCP/S->C"
echo "================================================"
echo


iperf3 -c ${ip} -p ${port} -b ${bandwidth} -i 1 -P 10 -t 10 -T "${bandwidth}/parallel=10/TCP/C->S"
echo "[EGGROLL] test finished (5/8): ${bandwidth}/parallel=10/TCP/C->S"
echo "================================================"
echo

iperf3 -c ${ip} -p ${port} -b ${bandwidth} -i 1 -P 10 -t 10 -R -T "${bandwidth}/parallel=10/TCP/S->C"
echo "[EGGROLL] test finished (6/8): ${bandwidth}/parallel=10/TCP/S->C"
echo "================================================"
echo

iperf3 -c ${ip} -p ${port} -b 1m -i 1 -P 10 -t 10 -u -T "1m/parallel=10/UDP/C->S"
echo "[EGGROLL] test finished (7/8): 1m/parallel=10/UDP/C->S"
echo "================================================"
echo

iperf3 -c ${ip} -p ${port} -b 1m -i 1 -P 10 -t 10 -u -R -T "1m/parallel=10/UDP/S->C"
echo "[EGGROLL] test finished (8/8): 1m/parallel=10/UDP/S->C"
echo "================================================"
echo


echo "[EGGROLL] iperf3 tests finished"
