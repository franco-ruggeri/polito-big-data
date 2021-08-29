package it.polito.bigdata.hadoop.lab01;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper for words
 */
class MapperBigDataWord extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Split each sentence in words. Use whitespace(s) as delimiter 
		// (=a space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] words = value.toString().split("\\s+");
		
		// Iterate over the set of words
		for (String word : words) {
			// Transform word case
			String cleanedWord = word.toLowerCase();

			// emit the pair (word, 1)
			context.write(new Text(cleanedWord), new IntWritable(1));
		}
	}
}