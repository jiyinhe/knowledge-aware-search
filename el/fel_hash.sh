# This script creates the hash from the entity counts

# Setup
FEL_HOME=~/downloads/FEL/

datapack_file=~/grid/tmp/wiki/fel/datapack/20151215/wiki-en-20161120/features.dat
entity2id_file=~/grid/tmp/wiki/fel/datapack/20151215/wiki-en-20161120/id-entity.tsv
output_file=~/grid/tmp/wiki/fel/datapack/20151215/wiki-en-20161120/wiki-en-20161120.hash

export HADOOP_OPTS="-Xmx5g" 

java -Xmx4g -cp ${FEL_HOME}/target/FEL-0.1.0-fat.jar \
    com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash \
    -i $datapack_file \
    -e $entity2id_file \
    -o $output_file
