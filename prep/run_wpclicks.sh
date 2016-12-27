#!/bin/bash

if [[ $# -lt 1 ]]; then
	echo 'Usage run.sh stats'
	echo 'stats: [tot|tra|sta]-total click counts; tra-transition probability; sta-stationary probability'
	exit
fi

stats=$1
tot=0

output_localdir=../data/WPclick/
STREAM_DIR=~/hadoop-2.7.3/share/hadoop/tools/lib/

inputdir=logs/WPclickstream/
outputdir=tmp_output_wpclicks/

input=${inputdir}
output=${outputdir}/all_${stats}
output_local=${output_localdir}/all_${stats}

# Find total counts of clicks if computing stationary probability
if [[ $stats == 'sta' ]]; then
	tot=`cat ${output_localdir}/all_tot/*`
fi

hdfs dfs -rm -r $output

hadoop jar $STREAM_DIR/hadoop-*streaming*.jar \
-D mapred.reduce.tasks=10 \
-files WPClicks.py \
-mapper "WPClicks.py  map $stats -t $tot" \
-reducer "WPClicks.py reduce $stats -t $tot" \
-input /user/he/$input -output /user/he/$output 

# first clean the outputdir if it exists
if [ -e $output_local ]; then
	rm -r $output_local
fi

# get the results
hdfs dfs -get $output $output_local

