#!/bin/bash
cat input/data/"$(basename "$0" | cut -d'.' -f 1)".csv
