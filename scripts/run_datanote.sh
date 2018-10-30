#!/bin/bash

for val in 0{1..9} 10
do
    echo VM$val
    ssh kechenl3@fa18-cs425-g29-$val.cs.illinois.edu "cd ~/go/src/simpledfs/datanode; go build; ./datanode > /dev/null 2>&1 &"
    echo VM$val DataNode Started
done

