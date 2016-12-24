#!/bin/bash

#This script runs the pre-processing of logs
#A log is processed in 3 phases:
#phase 1: parse the log and extract information needed for the final format
#phase 2: parse the log and format it for entity linking
#phase 3: collect results from phase 1 and 2, and format the final result


if [[ $# -lt 2 ]]; then
	echo 'Usage run.sh logname[AOL|KB] phase[1|2|3]'
	echo 'logname: the logs that can be processed: AOL, KB'
	echo 'phase 1: parse the log and extract information needed for the final format'
	echo 'phase 2: parse the log and do entity linking'
	echo 'phase 3: collect results from phase 1 and 2, and format the final result'
	exit
fi

log=$1
phase=$2

output_localdir=../data/
input_AOL=logs/AOL-user-ct-collection/


STREAM_DIR=~/hadoop-2.7.3/share/hadoop/tools/lib/

if [[ $log=='AOL' ]]; then
	input=${input_AOL}/*.gz
	output=tmp_output_${log}_phase${phase}
	output_local=${output_localdir}/output_${log}_phase${phase}
fi

# first clean the outputdir if it exists
hdfs dfs -rm -r $output
if [ -e $output_local ]; then
	rm -r $output_local
fi

hadoop jar $STREAM_DIR/hadoop-*streaming*.jar \
-files logParser.py \
-mapper "logParser.py ${log} map -p ${phase}" \
-reducer "logParser.py ${log} reduce -p ${phase}" \
-input /user/he/$input -output /user/he/$output \
-jobconf mapred.reduce.tasks=100 


hdfs dfs -get $output $output_local

