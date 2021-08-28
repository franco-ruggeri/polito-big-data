package it.polito.bigdata.hadoop.lab04;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapReduce - Mapper of Job 1
 */
class MapperBigData1 extends Mapper<LongWritable,Text,Text,RatingWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		
		// filter out header
		String id = fields[0];
		if (id.equals("Id"))
			return;
		
		// emit key-value pairs to group by user ID
		String productId = fields[1];
		String userId = fields[2];
		int score = Integer.parseInt(fields[6]);
		RatingWritable rating = new RatingWritable(productId, score);
		context.write(new Text(userId), rating);
	}
}


