# This script downloads a WP dump to local directory
#!/bin/bash

# set WIKI_MARKET to the wikipedia dump language, e.g.
WIKI_MARKET=en

# set WIKI_DATE to the wikipedia dump you want to process, e.g.
WIKI_DATE=20161120

#set your WORKING_DIR to the directory you want to store the data
WORKING_DIR=~/grid/tmp/wiki

# create directory
mkdir --parents ${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}

# download datapack from web
 wget --output-document=${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml.bz2 \
http://dumps.wikimedia.org/\
${WIKI_MARKET}wiki/${WIKI_DATE}/${WIKI_MARKET}wiki-${WIKI_DATE}-pages-articles.xml.bz2

# unzip datapack
bzip2 --verbose --keep --decompress \
${WORKING_DIR}/${WIKI_MARKET}/${WIKI_DATE}/pages-articles.xml.bz2

