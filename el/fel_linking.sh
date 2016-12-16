# This script run FEL in distributed mode

# Set up
FEL_HOME=~/downloads/FEL
DATA_LOCAL=~/grid/tmp/wiki/fel/datapack/20151215/wiki-en-20161120
HASH_FILE=wiki-en-20161120.hash
IDMAP_FILE=id-type.tsv

HDFS_DIR=/user/he/fel/20151215

if [[ $# -lt 2 ]]; then
	echo 'Usage fel_linking.sh inputfile outputfile'
	exit 1
fi

inputfile=$1
outputfile=$2

# copy the hash and id mapping files to hdfs
hdfs dfs -put ${DATA_LOCAL}/${HASH_FILE} ${HDFS_DIR}/${HASH_FILE}
hdfs dfs -put ${FEL_HOME}/src/main/bash/${IDMAP_FILE} ${HDFS_DIR}/${IDMAP_FILE}

# copy the inputfile to hdfs
hdfs dfs -put $inputfile input

hadoop jar ${FEL_HOME}/target/FEL-0.1.0-fat.jar \
    com.yahoo.semsearch.fastlinking.utils.RunFELOntheGrid \
    -Dmapred.map.tasks=100 \
    -Dmapreduce.map.java.opts=-Xmx3g \
    -Dmapreduce.map.memory.mb=3072 \
    -files hdfs://emeralds01:9000/${HDFS_DIR}/${HASH_FILE}#hash,hdfs://emeralds01:9000${HDFS_DIR}/${IDMAP_FILE}#mapping \
    input output

#    -Dmapred.job.queue.name=adhoc \

hdfs dfs -get output ${outputfile}

# Cleanup
hdfs dfs -rm ${HDFS_DIR}/${IDMAP_FILE}
hdfs dfs -rm ${HDFS_DIR}/${HASH_FILE}
hdfs dfs -rm -r input
hdfs dfs -rm -r output

