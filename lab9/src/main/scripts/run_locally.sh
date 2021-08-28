# Check arguments
if [ $# -lt 2 ]; then
	echo "Usage: $0 algorithm feature1 [feature2 ...]"
	exit 1
fi

# Remove folders of the previous run
rm -rf results

# Run application
spark-submit --class it.polito.bigdata.spark.lab9.SparkDriver --deploy-mode client --master local target/Lab9-1.0.0.jar data results $@
