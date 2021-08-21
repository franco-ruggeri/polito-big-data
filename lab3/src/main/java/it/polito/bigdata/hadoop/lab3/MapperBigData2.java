package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapReduce - Mapper of Job 2
 * 
 * Transparent mapper. Since the computation of the local top K pairs is anticipated in the reducers of the first job,
 * this Mapper need just to output the the key-value pairs as they are.
 */
class MapperBigData2 extends Mapper<Text,Text,NullWritable,RecordCountWritable> {
	
	@Override
	protected void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException {
		String record = key.toString();
		int count = Integer.parseInt(value.toString());
		RecordCountWritable recordCount = new RecordCountWritable(record, count);
		context.write(NullWritable.get(), recordCount);
	}
}
