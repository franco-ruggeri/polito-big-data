package it.polito.bigdata.hadoop.lab3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapReduce - Mapper of Job 1
 * 
 * Implementation of the numerical summarization pattern.
 */
class MapperBigData1 extends Mapper<LongWritable,Text,PairWritable,IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		for (int i=1; i<fields.length; i++) {
			for (int j=i+1; j<fields.length; j++) {
				if (!fields[i].equals(fields[j])) {
					PairWritable newKey = new PairWritable(fields[i], fields[j]);
					context.write(newKey, new IntWritable(1));
				}
			}
		}
	}
}
