package it.polito.bigdata.hadoop.lab4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapReduce - Mapper of Job 2
 */
class MapperBigData2 extends Mapper<Text,Text,Text,AverageWritable> {

	@Override
	protected void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException {
		double rating = Double.parseDouble(value.toString());
		context.write(key, new AverageWritable(rating, 1));
	}
}


