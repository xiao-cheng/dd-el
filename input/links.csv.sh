if [ ! -f input/chunks/link0.csv ]; then
  bash input/parse.sh
fi
cat input/chunks/link*.csv

