# Check arguments
if [ $# -ne 1 ]; then
	echo "Usage: $0 threshold"
	exit 1
fi

# Remove output folder of the previous run
hdfs dfs -rm -r results

# Run application
spark-submit --class it.polito.bigdata.spark.lab8.SparkDriver --deploy-mode cluster --master yarn Lab8-1.0.0.jar /data/students/bigdata-01QYD/Lab7/register.csv /data/students/bigdata-01QYD/Lab7/stations.csv results $@

# Retrieve result
hdfs dfs -getmerge results result.txt
