bzcat /Users/xiaocheng/Downloads/enwiki-sample-pages-articles.xml.bz2 | mvn -q -f udf/wikiapi/pom.xml exec:java | deepdive sql "COPY wiki_pages from STDIN"
