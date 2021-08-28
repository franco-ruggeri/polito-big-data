package it.polito.bigdata.hadoop.lab04;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MapReduce - Reducer of Job 1
 */
class ReducerBigData1 extends Reducer<Text,RatingWritable,NullWritable,RatingWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<RatingWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		// compute average rating of the user
		double avg = 0;
		int count = 0;
		List<RatingWritable> ratings = new LinkedList<>();
		for (RatingWritable v : values) {
			avg += v.getScore();
			count++;
			
			// store for second iteration, must copy, no references! (see lab3)
			ratings.add(new RatingWritable(v));
		}
		avg /= count;
		
		// emit normalized ratings
		for (RatingWritable r : ratings) {
			r.normalizeScore(avg);
			context.write(NullWritable.get(), r);
		}
	}
}
