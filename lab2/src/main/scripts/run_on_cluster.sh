if [ $# -ne 2 ]; then
	echo "Usage: $0 filter word|bigram"
	exit 1
fi

# Remove folders of the previous run
hdfs dfs -rm -r data
hdfs dfs -rm -r results

# Put input data collection into hdfs
hdfs dfs -put data

# Run application
hadoop jar Lab2-1.0.0.jar it.polito.bigdata.hadoop.lab2.DriverBigData data results $@

# Retrieve result
hadoop dfs -getmerge results result.txt
