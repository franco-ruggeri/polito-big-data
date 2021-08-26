# Check arguments
if [ $# -ne 1 ]; then
	echo "Usage: $0 threshold"
	exit 1
fi

# Remove folders of the previous run
rm -rf results

# Run application
spark-submit --class it.polito.bigdata.spark.lab7.SparkDriver --deploy-mode client --master local target/Lab7-1.0.0.jar data/registerSample.csv data/stations.csv results $@
