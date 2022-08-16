#!/bin/bash

# change to script directory
cd "${0%/*}" || exit

for i in "1" "2" "5"
do
   go run . -name "SIFT" -numberEntries 100000 -fail "-1" -numBatches $i
done
