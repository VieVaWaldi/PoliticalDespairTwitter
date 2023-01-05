#!/bin/bash
# cat user_list.csv

while IFS="," read -r rec_column1 rec_column2 rec_column3 rec_column4 rec_column5
do
  python GetOldTweets3/cli.py --username $rec_column3 > data/$rec_column3
  
done < <(tail -n +2 user_list.csv)

# python GetOldTweets3/cli.py --username "BarackObama"