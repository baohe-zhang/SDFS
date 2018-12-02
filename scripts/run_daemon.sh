#!/bin/bash

for val in 0{1..9} 10
do
    echo VM$val
    #ssh kechenl3@fa18-cs425-g29-$val.cs.illinois.edu "cd ~/go/src/simpledfs/datanode; go build; ./datanode > /dev/null 2>&1 &"
    ssh kechenl3@fa18-cs425-g29-$val.cs.illinois.edu "cd ~/go/src/simpledfs/daemon; go build; ./daemon -master fa18-cs425-g29-01.cs.illinois.edu > ./daemon.log 2>&1 &"
    sleep 3
    echo VM$val Distributed File System Daemon Started
done

