# Remove folders of the previous run
hdfs dfs -rm -r data
hdfs dfs -rm -r results

# Put input data collection into hdfs
hdfs dfs -put data

# Run application
hadoop jar Lab04-1.0.0.jar it.polito.bigdata.hadoop.lab04.DriverBigData 2 2 data results/norm_ratings results/norm_avg_ratings

# Retrieve result
hdfs dfs -getmerge results/norm_avg_ratings result.txt
