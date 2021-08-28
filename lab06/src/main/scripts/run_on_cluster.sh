# Remove output folder of the previous run
hdfs dfs -rm -r results

# Run application
spark-submit --class it.polito.bigdata.spark.lab06.SparkDriver --deploy-mode client --master yarn Lab06-1.0.0.jar /data/students/bigdata-01QYD/Lab4/Reviews.csv results 

# Retrieve result
hdfs dfs -getmerge results result.txt
