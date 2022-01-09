#!/bin/bash
go run mapreduce/wordcount data/pg-*.txt

if [ $? -ne 0 ]; then
  exit
fi

sort -n -k2 mrtmp.wordcount-sequential | tail -10 | diff - data/wc-testout.txt > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in wc-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo "Passed test" > /dev/stderr
fi

