package it.polito.bigdata.spark;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	
	public static void main(String[] args) throws InterruptedException {

		
		String outputPathPrefix;
		String inputFolder;

		inputFolder = args[0];
		outputPathPrefix = args[1];
	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Streaming Lab 10");
				
		// Create a Spark Streaming Context object
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));		
		
		// Set the checkpoint folder (it is needed by some window transformations)
		jssc.checkpoint("checkpointfolder");

		JavaDStream<String> tweets = jssc.textFileStream(inputFolder);

		// TODO
		// Process the tweets JavaDStream.
		// Every time a new file is uploaded  in inputFolder a new set of streaming data 
		// is generated 
		// ...


		
		// Start the computation
		jssc.start();              
		
		// Run the application for at most 120000 ms
		jssc.awaitTerminationOrTimeout(120000);
		
		jssc.close();
		
	}
}
