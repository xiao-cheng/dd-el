if [ ! -f input/chunks/page0.csv ]; then
  bash input/parse.sh
fi
cat input/chunks/page*.csv

