# This script runs the yahoo fast entity linking tool.
# Note: the job history server should be running for pig

# set WIKI_MARKET to the wikipedia dump language, e.g.
WIKI_MARKET=en

# set WIKI_DATE to the wikipedia dump you want to process, e.g.
WIKI_DATE=20161120

# set FEL_DATE to the FEL datapack generation date, e.g.
FEL_DATE=20151215


#set your WORKING_DIR to the directory you want to store the data
WORKING_DIR=~/grid/tmp/wiki

#set JAVA_HOME
JAVA_HOME=/usr/

#set EFL home
FEL_HOME=~/downloads/FEL/

# create hdfs directory
hdfs dfs -mkdir -p wiki/${WIKI_MARKET}/${WIKI_DATE}

# copy datapack on hdfs
hdfs dfs -copyFromLocal \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
wiki/${WIKI_MARKET}/${WIKI_DATE}/

# Prepare datapack

hadoop \
jar $FEL_HOME/target/FEL-0.1.0-fat.jar \
com.yahoo.semsearch.fastlinking.io.WikipediaDocnoMappingBuilder \
-Dmapreduce.map.env="JAVA_HOME=$JAVA_HOME" \
-Dmapreduce.reduce.env="JAVA_HOME=$JAVA_HOME" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=$JAVA_HOME" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
-output_file wiki/${WIKI_MARKET}/${WIKI_DATE}/docno.dat \
-wiki_language ${WIKI_MARKET} \
-keep_all

hadoop \
jar $FEL_HOME/target/FEL-0.1.0-fat.jar \
com.yahoo.semsearch.fastlinking.io.RepackWikipedia \
-Dmapreduce.map.env="JAVA_HOME=$JAVA_HOME" \
-Dmapreduce.reduce.env="JAVA_HOME=$JAVA_HOME" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=$JAVA_HOME" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml \
-mapping_file wiki/${WIKI_MARKET}/${WIKI_DATE}/docno.dat \
-output wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
-wiki_language ${WIKI_MARKET} \
-compression_type block

#Build Data Structures and extract anchor text

hadoop \
jar $FEL_HOME/target/FEL-0.1.0-fat.jar \
com.yahoo.semsearch.fastlinking.io.ExtractWikipediaAnchorText \
-Dmapreduce.map.env="JAVA_HOME=$JAVA_HOME" \
-Dmapreduce.reduce.env="JAVA_HOME=$JAVA_HOME" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=$JAVA_HOME" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
-emap wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map \
-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
-redir wiki/${WIKI_MARKET}/${WIKI_DATE}/redirects

# Run it again for phase2
hadoop \
jar $FEL_HOME/target/FEL-0.1.0-fat.jar \
com.yahoo.semsearch.fastlinking.io.ExtractWikipediaAnchorText \
-Dmapreduce.map.env="JAVA_HOME=$JAVA_HOME" \
-Dmapreduce.reduce.env="JAVA_HOME=$JAVA_HOME" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=$JAVA_HOME" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
-emap wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map \
-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
-redir wiki/${WIKI_MARKET}/${WIKI_DATE}/redirects \
-phase 2

# Run it again for phase 3 and merge
hadoop \
jar $FEL_HOME/target/FEL-0.1.0-fat.jar \
com.yahoo.semsearch.fastlinking.io.ExtractWikipediaAnchorText \
-Dmapreduce.map.env="JAVA_HOME=$JAVA_HOME" \
-Dmapreduce.reduce.env="JAVA_HOME=$JAVA_HOME" \
-Dyarn.app.mapreduce.am.env="JAVA_HOME=$JAVA_HOME" \
-Dmapred.job.map.memory.mb=6144 \
-Dmapreduce.map.memory.mb=6144 \
-Dmapred.child.java.opts="-Xmx2048m" \
-Dmapreduce.map.java.opts='-Xmx2g -XX:NewRatio=8 -XX:+UseSerialGC' \
-input wiki/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.block \
-emap wiki/${WIKI_MARKET}/${WIKI_DATE}/entities.map \
-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
-redir wiki/${WIKI_MARKET}/${WIKI_DATE}/redirects \
-phase 3


#Compute anchor text counts

hadoop \
jar $FEL_HOME/target/FEL-0.1.0-fat.jar \
com.yahoo.semsearch.fastlinking.io.Datapack \
-amap wiki/${WIKI_MARKET}/${WIKI_DATE}/anchors.map \
-cfmap wiki/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.map \
-multi true \
-output ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts

# copy to hdfs
hdfs dfs -copyFromLocal \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.dat \
wiki/${WIKI_MARKET}/${WIKI_DATE}/

# copy to hdfs
hdfs dfs -copyFromLocal \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.tsv \
wiki/${WIKI_MARKET}/${WIKI_DATE}/

# create directory
hdfs dfs -mkdir -p \
wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count

# copy counts
hdfs dfs -copyFromLocal ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/alias-entity-counts.tsv \
wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count

# set numerical id
hdfs dfs -text wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count/* | \
cut --fields 4 | \
LC_ALL=C sort --dictionary-order | \
LC_ALL=C uniq | \
awk '{print $0"\t"NR}' | \
hdfs dfs -put - wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv



#Aggregate Alias-Entity Dependent Counts

#pig \
#-stop_on_failure \
#-Dpig.additional.jars=$FEL_HOME/target/FEL-0.1.0-fat.jar \
#-Dmapred.output.compression.enabled=true \
#-Dmapred.output.compress=true \
#-Dmapred.output.compression.type=BLOCK \
#-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
#-Dmapred.job.queue.name=adhoc \
#-param feat=wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count \
#-param output=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/agg \
#-file $FEL_HOME/src/main/pig/aggregate-graph-alias-entity-counts.pig

pig \
-Dpig.additional.jars=$FEL_HOME/target/FEL-0.1.0-fat.jar \
-param feat=wiki/${WIKI_MARKET}/${WIKI_DATE}/feat/alias-entity/count \
-param output=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/agg \
-file $FEL_HOME/src/main/pig/aggregate-graph-alias-entity-counts.pig


pig \
-stop_on_failure \
-Dmapred.output.compression.enabled=true \
-Dmapred.output.compress=true \
-Dmapred.output.compression.type=BLOCK \
-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
-Dmapred.job.queue.name=adhoc \
-param counts=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/agg \
-param output=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/final \
-file $FEL_HOME/src/main/pig/compute-graph-alias-entity-counts.pig


#Generate Feature Vectors

# This script would merge Search-Based Counts and Graph-Based Counts 
# (or different counts) - currently the search based counts are set to zero.

# set WIKI_MARKET to the wikipedia dump language, e.g.
WIKI_MARKET=en

#time \
#pig \
#-stop_on_failure \
#-useHCatalog \
#-Dpig.additional.jars=$FEL_HOME/target/FEL-0.1.0-fat.jar \
#-Dmapred.output.compression.enabled=true \
#-Dmapred.output.compress=true \
#-Dmapred.output.compression.type=BLOCK \
#-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
#-Dmapred.job.queue.name=adhoc \
#-param entity=wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv \
#-param graph=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/final \
#-param output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/final \
#-param err_output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/log/final \
#-file $FEL_HOME/src/main/pig/join-alias-entity-counts.pig \
#>& join-alias-entity-counts.log

pig \
-Dpig.additional.jars=$FEL_HOME/target/FEL-0.1.0-fat.jar \
-param entity=wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv \
-param graph=fel/${FEL_DATE}/feat/graph/${WIKI_MARKET}/${WIKI_DATE}/alias-entity/final \
-param output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/final \
-param err_output=fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/log/final \
-file $FEL_HOME/src/main/pig/join-alias-entity-counts.pig \
>& join-alias-entity-counts.log

# Copy Datapack to Local Directory

OUTPUT_DIR=${WORKING_DIR}/fel/datapack/${FEL_DATE}/wiki-${WIKI_MARKET}-${WIKI_DATE}

mkdir --parent ${OUTPUT_DIR}

hdfs dfs -copyToLocal \
wiki/${WIKI_MARKET}/${WIKI_DATE}/id-entity.tsv \
${OUTPUT_DIR}/

hdfs dfs -text \
fel/${FEL_DATE}/feat/all/${WIKI_MARKET}/final/* | \
sed "s/\t{(/\t/" | \
sed "s/),(/\t/g" | \
sed "s/)}$//" \
> ${OUTPUT_DIR}/features.dat

chmod --recursive ugo+rx \
${WORKING_DIR}/fel



