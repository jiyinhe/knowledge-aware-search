# Note: all python scripts to be run on hadoop should be put under
# the same directory. I put them under app. The import statements 
# should be changed accordingly as well.

if [[ $# -lt 1 ]]; then
	echo 'Usage run_etorder.sh logname[AOL|KB]'
	echo 'logname: the logs that can be processed: AOL, KB'
	exit
fi

log=$1

CODEHOME=~/knowledge-aware-search/
PROG=statsQueryEt

output_localdir=${CODEHOME}/data/

STREAM_DIR=~/hadoop-2.7.3/share/hadoop/tools/lib/

input1_local=${output_localdir}output_${log}_phase4
input2_local=~/grid/tmp/wiki/clickstream

input=tmp_input_${log}_${PROG}

hdfs dfs -mkdir $input
hdfs dfs -put $input1_local $input
hdfs dfs -put $input2_local $input


output=tmp_output_${log}_${PROG}
output_local=${output_localdir}/output_${log}_${PROG}

# first clean the outputdir if it exists
hdfs dfs -rm -r $output

hadoop jar $STREAM_DIR/hadoop-*streaming*.jar \
-D mapreduce.reduce.tasks=100 \
-files app \
-mapper "app/statsQueryEt.py map" \
-reducer "app/statsQueryEt.py reduce" \
-input /user/he/$input/*/* -output /user/he/$output 

# get the results
if [ -e $output_local ]; then
	rm -r $output_local
fi

hdfs dfs -get $output $output_local

