package it.polito.bigdata.hadoop.lab03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MapReduce - Reducer of Job 1
 * 
 * Starting from the numerical summarization pattern, but exploiting the knowledge of the overall goal,
 * the top K operation is anticipated here to output only the local top K pairs instead of all. 
 * This is an optimization because the reducers emit less key-value pairs.
 */
class ReducerBigData1 extends Reducer<PairWritable,IntWritable,NullWritable,RecordCountWritable> {
	private TopKVector<RecordCountWritable> localTopK;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		int k = config.getInt("k", 0);
		localTopK = new TopKVector<>(k);
	}
	
	@Override
	protected void reduce(PairWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable v : values)
			sum += v.get();
		RecordCountWritable recordCount = new RecordCountWritable(key.toString(), sum);
		localTopK.update(recordCount);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		for (RecordCountWritable e : localTopK.getTopK())
			context.write(NullWritable.get(), e);
	}
}
