#!/bin/bash

#This script runs the pre-processing of logs
#A log is processed in 3 phases:
#phase 1: parse the log and extract information needed for the final format
#phase 2: parse the log and format it for entity linking
#phase 3: collect results from phase 1 and 2, and format the final result


if [[ $# -lt 1 ]]; then
	echo 'Usage run.sh phase [1|2|3]'
	echo 'phase 1: parse the log and extract information needed for the final format'
	echo 'phase 2: parse the log and do entity linking'
	echo 'phase 3: collect results from phase 1 and 2, and format the final result'
	exit
fi

phase=$1

if [ $phase == 1 ]; then
	

elif [ $phase == 2 ]; then
	echo 'this is 2'
else
	echo $phase
fi

