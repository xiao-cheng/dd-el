if [ ! -f input/chunks/redirect0.csv ]; then
  bash input/parse.sh
fi
cat input/chunks/redirect*.csv

