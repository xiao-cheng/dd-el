#!/bin/bash
XML_DUMP=/lfs/local/0/xiao/enwiki-20160204-pages-articles.xml.bz2
#XML_DUMP=./dump.xml.bz2
if [[ "$(uname)" == 'Darwin' ]]; then
   XML_DUMP=~/Downloads/enwiki-sample-pages-articles.xml.bz2
fi
cd input
if [ ! -f chunks/0.csv ]; then
  mvn -f wikiapi/pom.xml -q clean compile assembly:single
  bzcat $XML_DUMP | java -jar wikiapi/target/wikiapi*.jar
  # Make read-only
  chmod 400 chunks/*.csv
fi
