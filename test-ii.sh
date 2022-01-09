#!/bin/bash
go run mapreduce/invertedindex data/pg-*.txt

if [ $? -ne 0 ]; then
  exit
fi

LC_ALL=C
export LC_ALL

sort -k1,1 mrtmp.ii-sequential | sort -snk2,2 | grep -v '16' | tail -10 | diff - data/ii-testout.txt > diff.out
if [ -s diff.out ]
then
echo "Output should be as in ii-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo "Test passed!" > /dev/stderr
fi

