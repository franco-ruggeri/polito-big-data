package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MapReduce - Reducer
 * 
 * Implementation of the top K pattern.
 */
class ReducerTopK extends Reducer<NullWritable,RecordCountWritable,NullWritable,RecordCountWritable> {
	private int k;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		k = config.getInt("k", 0);
	}
	
    @Override
    protected void reduce(NullWritable key, Iterable<RecordCountWritable> values, Context context) 
    		throws IOException, InterruptedException {
    	TopKVector<RecordCountWritable> topK = new TopKVector<>(k);
    	
    	for (RecordCountWritable v : values) {
    		/* 
    		 * Important: copy object.
    		 * 
    		 * This is needed because the Iterable in Hadoop loads the values always in the same object.
    		 * So, if we don't copy the object at each iteration, we would store in the top K vector references to the same object.
    		 * After the loop, such an object contains the last value, so the top K would contain many references to the last value.
    		 */
    		RecordCountWritable vCopy = new RecordCountWritable(v);
    		topK.update(vCopy);
    	}
    	
    	for (RecordCountWritable e : topK.getTopK())
			context.write(NullWritable.get(), e);
    }
}
