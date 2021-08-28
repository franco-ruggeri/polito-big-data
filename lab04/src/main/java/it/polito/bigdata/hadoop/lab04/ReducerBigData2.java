package it.polito.bigdata.hadoop.lab04;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MapReduce - Reducer of Job 2
 */
class ReducerBigData2 extends Reducer<Text,AverageWritable,Text,AverageWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<AverageWritable> values, Context context) 
			throws IOException, InterruptedException {
		// compute average
		float sum = 0;
		int count = 0;
		for (AverageWritable v : values) {
			sum += v.getSum();
			count += v.getCount();
		}
		AverageWritable avg = new AverageWritable(sum, count);
		
		// emit key-value pair
		context.write(new Text(key), avg);
	}
}
