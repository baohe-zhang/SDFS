#!/bin/bash

for val in 0{1..9} 10
do
    echo VM$val
    ssh kechenl3@fa18-cs425-g29-$val.cs.illinois.edu "pkill daemon"
    echo VM$val Daemon Killed
done

