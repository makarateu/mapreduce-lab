#!/bin/bash
go run mapreduce/invertedindex -distributed data/pg-*.txt &
pid=$!
sleep 0.2
go run mapreduce/invertedindex -worker -workerAddr localhost:7777 &
go run mapreduce/invertedindex -worker -workerAddr localhost:7778 &
go run mapreduce/invertedindex -worker -workerAddr localhost:7779 &

wait $pid

if [ $? -ne 0 ]; then
  exit
fi

sort -n -k2 mrtmp.ii-parallel | tail -10 | diff - data/ii-testout.txt > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in wc-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo "Passed test" > /dev/stderr
fi

