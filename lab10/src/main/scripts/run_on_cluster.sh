# Check arguments
if [ $# -ne 1 ]; then
	echo "Usage: $0 sleep"
	exit 1
fi

# Remove folders of the previous run
hdfs dfs -rm -r data results*

# Create data folder
hdfs dfs -mkdir data

# Run application
spark-submit --class it.polito.bigdata.spark.lab10.SparkDriver --deploy-mode client --master yarn Lab10-1.0.0.jar data results &

# Put files into data folder
sleep 15
hdfs dfs -put data/tweets_blockaa data
sleep $1
hdfs dfs -put data/tweets_blockab data
sleep $1
hdfs dfs -put data/tweets_blockac data
sleep $1
hdfs dfs -put data/tweets_blockad data
