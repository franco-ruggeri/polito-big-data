# Remove folders of the previous run
hdfs dfs -rm -r data
hdfs dfs -rm -r results

# Put input data collection into hdfs
hdfs dfs -put data

# Run application
hadoop jar Lab3-1.0.0.jar it.polito.bigdata.hadoop.lab3.DriverBigData 2 data results/count results/top100 100

# Retrieve result
hdfs dfs -getmerge results/top100 result.txt
