# Remove folders of the previous run
hdfs dfs -rm -r data
hdfs dfs -rm -r results

# Put input data collection into hdfs
hdfs dfs -put data

# Run application
hadoop jar Lab1-1.0.0.jar it.polito.bigdata.hadoop.DriverBigData 2 data results
