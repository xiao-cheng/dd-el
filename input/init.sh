XML_DUMP=/dfs/scratch0/xiao/enwiki-20160204-pages-articles.xml.bz2
bzcat $XML_DUMP | mvn -q -f udf/wikiapi/pom.xml compile exec:java | deepdive sql "COPY wiki_pages from STDIN DELIMITER ',' CSV"
