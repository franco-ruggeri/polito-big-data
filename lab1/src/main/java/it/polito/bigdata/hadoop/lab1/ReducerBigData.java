package it.polito.bigdata.hadoop.lab1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData<T extends WritableComparable<? extends T>> extends Reducer<T,IntWritable,T,IntWritable> {
	
	@Override
	protected void reduce(T key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int occurrences = 0;

		// Iterate over the set of values and sum them 
		for (IntWritable value : values) {
			occurrences = occurrences + value.get();
		}

		context.write(key, new IntWritable(occurrences));
	}
}
