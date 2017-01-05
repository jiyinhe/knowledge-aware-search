#!/bin/bash

output_local=~/knowledge-aware-search/data/output_countTransWPclicks
STREAM_DIR=~/hadoop-2.7.3/share/hadoop/tools/lib/

input=logs/WPclickstream/
output=tmp_output_countTransWPclicks/


# Remove output if exists
hdfs dfs -rm -r $output

hadoop jar $STREAM_DIR/hadoop-*streaming*.jar \
-D mapred.reduce.tasks=10 \
-files countTransWPclicks.py \
-mapper "countTransWPclicks.py  map" \
-reducer "countTransWPclicks.py reduce" \
-input /user/he/$input -output /user/he/$output 

# first clean the outputdir if it exists
if [ -e $output_local ]; then
	rm -r $output_local
fi

# get the results
hdfs dfs -get $output $output_local

