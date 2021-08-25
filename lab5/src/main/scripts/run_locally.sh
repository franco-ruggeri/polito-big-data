# Check arguments
if [ $# -ne 1 ]; then
	echo "Usage: $0 prefix"
	exit 1
fi

# Remove folders of the previous run
rm -rf results

# Run application
spark-submit --class it.polito.bigdata.spark.lab5.SparkDriver --deploy-mode client --master local target/Lab5-1.0.0.jar data/SampleLocalFile.csv results $@
