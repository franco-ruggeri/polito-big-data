# Check arguments
if [ $# -ne 1 ]; then
	echo "Usage: $0 prefix"
	exit 1
fi

# Remove output folder of the previous run
hdfs dfs -rm -r results

# Run application
spark-submit --class it.polito.bigdata.spark.lab5.SparkDriver --deploy-mode client --master yarn Lab5-1.0.0.jar /data/students/bigdata-01QYD/Lab2/ results $@

# Retrieve result
hdfs dfs -getmerge results result.txt
