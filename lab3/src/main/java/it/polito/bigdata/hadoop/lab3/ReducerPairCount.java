package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MapReduce - Reducer
 * 
 * Implementation of the numerical summarization pattern.
 */
class ReducerPairCount extends Reducer<PairWritable,IntWritable,PairWritable,IntWritable> {
	
	@Override
	protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable v : values)
			sum += v.get();
		IntWritable newValue = new IntWritable(sum);
		context.write(key, newValue);
	}
}
