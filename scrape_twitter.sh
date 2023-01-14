#!/bin/bash
# cat user_list.csv

while IFS="," read -r rec_column1 rec_column2 rec_column3 rec_column4 rec_column5
do
  echo "Writing to data_2/$rec_column3"
  python GetOldTweets3/cli.py --username $rec_column3 > data_2/$rec_column3
  
done < <(tail -n +2 user_list_2.csv)

# python GetOldTweets3/cli.py --username "BarackObama"