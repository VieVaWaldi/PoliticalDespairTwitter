#!/bin/bash
hdfs dfs -rm -r Temp
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input Data -output Temp
