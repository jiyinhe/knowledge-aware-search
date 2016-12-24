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

if [[ ${phase} == '3' ]];
then
# phase 3
input_phase1_local=${output_localdir}/output_${log}_phase1
input_phase2_local=${output_localdir}/output_${log}_el
input_phase3_dir=tmp_input_${log}_phase3/

hdfs dfs -mkdir ${input_phase3_dir}
hdfs dfs -put ${input_phase1_local} ${input_phase3_dir}
hdfs dfs -put ${input_phase2_local} ${input_phase3_dir}

hadoop jar $STREAM_DIR/hadoop-*streaming*.jar \
-D mapred.reduce.tasks=100 \
-files logParser.py \
-mapper "logParser.py ${log} map -p ${phase}" \
-reducer "logParser.py ${log} reduce -p ${phase}" \
-input /user/he/$input_phase3_dir/*/* -output /user/he/$output \

else 
# phase 1, 2
hadoop jar $STREAM_DIR/hadoop-*streaming*.jar \
-D mapred.reduce.tasks=100 \
-files logParser.py \
-mapper "logParser.py ${log} map -p ${phase}" \
-reducer "logParser.py ${log} reduce -p ${phase}" \
-input /user/he/$input -output /user/he/$output \

fi

# get the results
if [ -e $output_local ]; then
	rm -r $output_local
fi

hdfs dfs -get $output $output_local

