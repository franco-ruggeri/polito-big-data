package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapReduce - Mapper
 * 
 * Implementation of the top K pattern.
 */
class MapperTopK extends Mapper<Text,Text,NullWritable,RecordCountWritable> {
	private TopKVector<RecordCountWritable> topK;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = context.getConfiguration();
		int k = config.getInt("k", 0);
		topK = new TopKVector<>(k);
	}
	
	@Override
	protected void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException {
		String record = key.toString();
		int count = Integer.parseInt(value.toString());
		RecordCountWritable recordCount = new RecordCountWritable(record, count);
		topK.update(recordCount);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
		for (RecordCountWritable e : topK.getTopK())
			context.write(NullWritable.get(), e);
	}
}
