# Note: all python scripts to be run on hadoop should be put under
# the same directory. I put them under app. The import statements 
# should be changed accordingly as well.

if [[ $# -lt 1 ]]; then
	echo 'Usage run_descstat.sh logname[AOL|KB]'
	echo 'logname: the logs that can be processed: AOL, KB'
	exit
fi

log=$1
phase=$2

CODEHOME=~/knowledge-aware-search/
output_localdir=../data/

STREAM_DIR=~/hadoop-2.7.3/share/hadoop/tools/lib/

input=tmp_output_${log}_phase4
output=tmp_output_${log}_preanalysis_descstats
output_local=${output_localdir}/output_${log}_preanalysis_descstats

# first clean the outputdir if it exists
hdfs dfs -rm -r $output

hadoop jar $STREAM_DIR/hadoop-*streaming*.jar \
-D mapreduce.reduce.tasks=10 \
-files app \
-mapper "app/descstats.py map" \
-reducer "app/descstats.py reduce" \
-input /user/he/$input -output /user/he/$output 

# get the results
if [ -e $output_local ]; then
	rm -r $output_local
fi

hdfs dfs -get $output $output_local

