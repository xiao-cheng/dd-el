XML_DUMP=/dfs/scratch0/xiao/enwiki-20160204-pages-articles.xml.bz2i
if [[ "$(uname)" == 'Darwin' ]]; then
  XML_DUMP=~/Downloads/enwiki-sample-pages-articles.xml.bz2
fi 

bzcat $XML_DUMP | mvn -q -f input/wikiapi/pom.xml compile exec:java -Dexec.args="$(readlink -f input/)"

#cat input/pages.csv | deepdive sql "COPY wiki_pages from STDIN DELIMITER ',' CSV"
#cat input/redirects.csv | deepdive sql "COPY redirects from STDIN DELIMITER ',' CSV"
#cat input/links.csv | deepdive sql "COPY links from STDIN DELIMITER ',' CSV"

