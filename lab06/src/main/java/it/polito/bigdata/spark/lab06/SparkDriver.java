package it.polito.bigdata.spark.lab06;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	private static int K = 10;
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath = args[0];
		String outputPath = args[1];
		
		SparkConf conf = new SparkConf().setAppName("Lab 6 - Frequently bought or reviewed together with Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		/* Task 1 */
		JavaPairRDD<String,Iterable<String>> userProducts = context.textFile(inputPath)
				.filter(line -> !line.startsWith("Id,"))	// filter out header
				.mapToPair(line -> {
					String[] fields = line.split(",");
					String productId = fields[1];
					String userId = fields[2];
					return new Tuple2<>(userId, productId);
				})
				.distinct()
				.groupByKey();
		
		JavaPairRDD<Integer,ProductPair> frequencyProductPair = userProducts
				.flatMapToPair(pair -> {
					// products reviewed by one user fit in memory
					List<String> products = new ArrayList<String>();
					for (String product : pair._2)
						products.add(product);
					
					// same is true for product pairs
					List<Tuple2<ProductPair,Integer>> productPairFrequency = new ArrayList<>();
					for (int i=0; i<products.size(); i++) {
						for (int j=i+1; j<products.size(); j++) {
							ProductPair productPair = new ProductPair(products.get(i), products.get(j));
							productPairFrequency.add(new Tuple2<>(productPair, 1));
						}
					}
					return productPairFrequency.iterator();
				})
				.reduceByKey((frequency1, frequency2) -> frequency1 + frequency2)
				.filter(pair -> pair._2 > 1)
				.mapToPair(pair -> new Tuple2<>(pair._2, pair._1))
				.sortByKey(false)
				.cache();	// cache because it is reused in task 2
		
		frequencyProductPair.saveAsTextFile(outputPath);
		/* End Task 1 */
		
		/* Task 2 */
		List<Tuple2<Integer,ProductPair>> topK = frequencyProductPair.top(K, new FrequencyComparator());
		
		System.out.println("===== TOP " + K + " ======");
		topK.forEach(System.out::println);
		/* End Task 2 */

		context.close();
	}
}
