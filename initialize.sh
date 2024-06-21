#!/bin/bash
pip install geopandas
hdfs dfs -mkdir /raw
hdfs dfs -mkdir /raw/new
hdfs dfs -mkdir /raw/processed
hdfs dfs -mkdir /silver
hdfs dfs -mkdir /silver/new
hdfs dfs -mkdir /silver/processed
hdfs dfs -mkdir /gold
hdfs dfs -mkdir /data
hdfs dfs -copyFromLocal 50001.parquet /data
hdfs dfs -copyFromLocal customers.parquet /data
hdfs dfs -copyFromLocal employees.parquet /data
hdfs dfs -copyFromLocal medellin_neighborhoods.parquet /data
