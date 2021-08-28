# Check arguments
if [ $# -ne 1 ]; then
	echo "Usage: $0 word|bigram"
	exit 1
fi

# Remove folders of the previous run
hdfs dfs -rm -r data
hdfs dfs -rm -r results

# Put input data collection into hdfs
hdfs dfs -put data

# Run application
hadoop jar Lab01-1.0.0.jar it.polito.bigdata.hadoop.lab01.DriverBigData 2 data results $@

# Retrieve result
hdfs dfs -getmerge results result.txt
