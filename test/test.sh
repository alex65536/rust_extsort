#!/bin/bash

set -e

g++ gen.cpp -o gen -O2
./gen >input.txt
time cargo run --release <input.txt >output.txt
sort <input.txt >answer.txt
diff output.txt answer.txt && echo Passed
rm -f input.txt output.txt answer.txt gen
