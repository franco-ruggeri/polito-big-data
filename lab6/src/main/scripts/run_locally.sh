# Remove folders of the previous run
rm -rf results

# Run application
spark-submit --class it.polito.bigdata.spark.lab6.SparkDriver --deploy-mode client --master local target/Lab6-1.0.0.jar data/ReviewsSample.csv results
