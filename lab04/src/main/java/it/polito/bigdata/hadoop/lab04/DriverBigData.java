package it.polito.bigdata.hadoop.lab04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce - Driver
 * 
 * It implements the job chaining pattern.
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		int exitCode, numberOfReducers1, numberOfReducers2;
		Configuration conf;
		Job job;
		Path inputPath, outputDir1, outputDir2;
		
		numberOfReducers1 = Integer.parseInt(args[0]);
		numberOfReducers2 = Integer.parseInt(args[1]);
		inputPath = new Path(args[2]);
		outputDir1 = new Path(args[3]);
		outputDir2 = new Path(args[4]);
		
		conf = this.getConf();
		
		/* Job 1 - Pair count */
		job = Job.getInstance(conf);
		job.setJobName("Lab 3 - Normalize ratings by user proclivity");
		job.setJarByClass(DriverBigData.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MapperBigData1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RatingWritable.class);

		job.setReducerClass(ReducerBigData1.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(RatingWritable.class);
		
		// TODO: can I optimize with combiners?
		
		job.setNumReduceTasks(numberOfReducers1);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDir1);
		
		exitCode = job.waitForCompletion(true) ? 0 : 1;
		if (exitCode != 0) 
			return exitCode;
		/* End Job 1 */
		
		/* Job 2 - Top K */
		job = Job.getInstance(conf);
		job.setJobName("Lab 3 - Average rating for each product");
		job.setJarByClass(DriverBigData.class);
	
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MapperBigData2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageWritable.class);

		job.setReducerClass(ReducerBigData2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AverageWritable.class);
		
		job.setCombinerClass(ReducerBigData2.class);
		
		job.setNumReduceTasks(numberOfReducers2);
		
		FileInputFormat.addInputPath(job, outputDir1);
		FileOutputFormat.setOutputPath(job, outputDir2);
		
		exitCode = job.waitForCompletion(true) ? 0 : 1;
		/* End Job 2 */
	
		return exitCode;
	}

	public static void main(String args[]) 
			throws Exception {
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
		System.exit(res);
	}
}
