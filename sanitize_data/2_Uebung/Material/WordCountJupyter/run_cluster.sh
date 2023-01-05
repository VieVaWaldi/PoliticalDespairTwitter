#!/bin/bash
hdfs dfs -rm -r result_dir
time hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -combiner reducer.py -input test_dir -output result_dir
