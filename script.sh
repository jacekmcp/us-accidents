#!/bin/sh

rm -rf Data
hadoop fs -copyToLocal gs://projekt_pbd_1/Data .

beeline -u 'jdbc:hive2://localhost:10000 projekt projekt' -f Data/db_hive_script
hadoop fs -rm -r -f Data
hadoop fs -copyFromLocal Data


rm -rf projekt_jars
hadoop fs -copyToLocal gs://projekt_pbd_1/projekt_jars .

spark-submit --class com.example.bigdata.distances_w --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 projekt_jars/distances_w.jar Data


spark-submit --class com.example.bigdata.geo_w --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 projekt_jars/geo_w.jar Data


spark-submit --class com.example.bigdata.date_w --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 projekt_jars/date_w.jar Data


spark-submit --class com.example.bigdata.weather_w --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 projekt_jars/weather_w.jar Data

spark-submit --class com.example.bigdata.fakty_f --master yarn --num-executors 5 --driver-memory 512m --executor-memory 512m --executor-cores 1 projekt_jars/fakty_f.jar Data


