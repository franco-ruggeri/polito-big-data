package it.polito.bigdata.spark.lab05;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	private static final double PERCENTAGE = 0.8;
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath = args[0];
		String outputPath = args[1];
		String prefix = args[2];
		
		SparkConf conf = new SparkConf().setAppName("Lab 5 - Filter data and compute basic statistics with Spark");
		JavaSparkContext context = new JavaSparkContext(conf);

		/* Task 1 */
		LongAccumulator nWords = context.sc().longAccumulator();
		
		JavaRDD<String> linesFilteredByPrefix = context.textFile(inputPath)
				.filter(line -> {
					// double '\' because it is a regex, but a single '\' works as well
					// see https://stackoverflow.com/questions/3762347/understanding-regex-in-java-split-t-vs-split-t-when-do-they-both-wor
					String[] fields = line.split("\\t");
					String word = fields[0].toLowerCase();
					
					boolean satisfied = word.startsWith(prefix);
					if (satisfied)
						nWords.add(1);
					return satisfied;
				})
				.cache();	// cache because it is reused in task 2
	
		int maxFrequency = linesFilteredByPrefix.aggregate(
				Integer.MIN_VALUE,
				(frequency1, line) -> {
					String[] fields = line.split("\\t");
					Integer frequency2 = Integer.parseInt(fields[1]);
					return Math.max(frequency1, frequency2);
				}, 
				(frequency1, frequency2) -> Math.max(frequency1, frequency2));
		
		System.out.println("===== TASK 1 =====");
		System.out.println("Prefix: " + prefix);
		System.out.println("Number of words: " + nWords.value());
		System.out.println("Max frequency: " + maxFrequency);
		System.out.println("==================\n");
		/* End Task 1 */
		
		/* Task 2 */
		nWords.reset();
		
		JavaRDD<String> wordsFilteredByFrequency = linesFilteredByPrefix
				.filter(line -> {
					String[] fields =line.split("\\t");
					Integer frequency = Integer.parseInt(fields[1]);
					boolean satisfied = frequency > PERCENTAGE * maxFrequency;
					if (satisfied)
						nWords.add(1);
					return satisfied;
				})
				.map(line -> {
					String[] fields =line.split("\\t");
					String word = fields[0].toLowerCase();
					return word;
				});
		
		// important: before printing (otherwise the accumulator is wrong, lazy evaluation) 
		wordsFilteredByFrequency.saveAsTextFile(outputPath);
		
		System.out.println("===== TASK 2 =====");
		System.out.println("Prefix: " + prefix);
		System.out.println("Threshold (percentage of max frequency): " + PERCENTAGE);
		System.out.println("Number of words: " + nWords.value());
		System.out.println("==================");
		/* End Task 2 */

		context.close();
	}
}
