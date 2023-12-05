#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

#TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=lab2-7.cs.mcgill.ca:21823,lab2-13.cs.mcgill.ca:21823,lab2-23.cs.mcgill.ca:21823

java -cp $CLASSPATH:../task:.: DistProcess 
