package it.polito.bigdata.hadoop.lab1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapReduce Project - Mapper for 2-grams
 */
class MapperBigDataBigram extends Mapper<LongWritable,Text,BigramWritable,IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Split each sentence in words. Use whitespace(s) as delimiter 
		// (=a space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] words = value.toString().split("\\s+");
		
		// To lower case
		for (int i=0; i<words.length; i++)
			words[i] = words[i].toLowerCase();
	
		// Emit key-value pairs
		for (int i=0; i<words.length-1; i++) {
			BigramWritable bigram = new BigramWritable(words[i], words[i+1]);
			
			// Simple cleaning: keep only alphanumeric bigrams
			if (!bigram.isAlphanumeric())
				continue;
			
			context.write(bigram, new IntWritable(1));			
		}
	}
}
