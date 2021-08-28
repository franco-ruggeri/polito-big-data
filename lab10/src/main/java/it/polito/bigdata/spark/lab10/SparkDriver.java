package it.polito.bigdata.spark.lab10;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	private static Duration BATCH_DURATION = Durations.seconds(10);
	private static Duration SLIDING_DURATION = BATCH_DURATION;
	private static Duration WINDOW_LENGTH = BATCH_DURATION.times(3);
	private static int THRESHOLD_RELEVANCE = 100;
	private static int TIMEOUT = 120000;
	
	public static void main(String[] args) throws InterruptedException {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		String inputFolder = args[0];
		String outputPathPrefix = args[1];
	
		SparkConf conf=new SparkConf().setAppName("Lab 10 - Tweet analysis with Spark Streaming");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, BATCH_DURATION);
		streamingContext.checkpoint("/tmp/spark-streaming-checkpoint");

		JavaDStream<String> tweets = streamingContext.textFileStream(inputFolder);
		
		JavaPairDStream<Integer,String> hashtags = tweets
				// hashtag -> +1
				.flatMapToPair(line -> {
					String[] fields = line.split("\t");
					String text = fields[1];
					String[] words = text.split("\\s+");
					
					List<Tuple2<String,Integer>> ht = new LinkedList<>();
					for (String w : words)
						if (w.startsWith("#"))
							ht.add(new Tuple2<>(w, 1));
					
					return ht.iterator();
				})
				// hashtag -> count
				.reduceByKeyAndWindow((c1, c2) -> c1+c2, WINDOW_LENGTH, SLIDING_DURATION)
				// count -> hashtag
				.mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
				// sort counts in descending order
				.transformToPair(pairRDD -> pairRDD.sortByKey(false))
				.cache();
		
		hashtags.dstream().saveAsTextFiles(outputPathPrefix, "all");
		hashtags.print();
		
		JavaPairDStream<Integer,String> relevantHashtags = hashtags
				.filter(pair -> pair._1 >= THRESHOLD_RELEVANCE);
		
		relevantHashtags.dstream().saveAsTextFiles(outputPathPrefix, "relevant");
		relevantHashtags.print();
		
		streamingContext.start();
		streamingContext.awaitTerminationOrTimeout(TIMEOUT);
		streamingContext.close();
	}
}
