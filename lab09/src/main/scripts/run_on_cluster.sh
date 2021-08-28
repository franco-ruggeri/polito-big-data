# Check arguments
if [ $# -lt 2 ]; then
	echo "Usage: $0 algorithm feature1 [feature2 ...]"
	exit 1
fi

# Remove output folder of the previous run
hdfs dfs -rm -r results

# Run application
spark-submit --class it.polito.bigdata.spark.lab09.SparkDriver --deploy-mode client --master yarn Lab09-1.0.0.jar /data/students/bigdata-01QYD/Lab9/ReviewsNewfile.csv results $@

# Retrieve result
hdfs dfs -getmerge results result.txt
