XML_DUMP=/dfs/scratch0/xiao/enwiki-20160204-pages-articles.xml.bz2
if [[ "$(uname)" == 'Darwin' ]]; then
  XML_DUMP=~/Downloads/enwiki-sample-pages-articles.xml.bz2
fi
# Make sure data dir exists
if [[ -d input ]]; then
  mkdir -p input/data
fi
bzcat $XML_DUMP | mvn -q -f input/wikiapi/pom.xml compile exec:java -Dexec.args="input/data"
#cat input/pages.csv | deepdive sql "COPY wiki_pages from STDIN DELIMITER ',' CSV"
#cat input/redirects.csv | deepdive sql "COPY redirects from STDIN DELIMITER ',' CSV"
#cat input/links.csv | deepdive sql "COPY links from STDIN DELIMITER ',' CSV"

