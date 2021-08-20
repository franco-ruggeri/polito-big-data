package it.polito.bigdata.hadoop.lab3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import it.polito.bigdata.hadoop.lab3.DriverBigData;
import it.polito.bigdata.hadoop.lab3.MapperTopK;
import it.polito.bigdata.hadoop.lab3.ReducerTopK;

/**
 * MapReduce - Driver
 * 
 * It implements the job chaining pattern.
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		int exitCode, numberOfReducers, k;
		Configuration conf;
		Job job;
		Path inputPath, outputDirCount, outputDirTopK;
		
		conf = this.getConf();
		
		numberOfReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputDirCount = new Path(args[2]);
		outputDirTopK = new Path(args[3]);
		k = Integer.parseInt(args[4]);
		
		/* Job 1 - Pair count */
		job = Job.getInstance(conf);
		job.setJobName("Lab 3 - Pair count");
		job.setJarByClass(DriverBigData.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MapperPairCount.class);
		job.setMapOutputKeyClass(PairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(ReducerPairCount.class);
		job.setOutputKeyClass(PairWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(ReducerPairCount.class);
		
		job.setNumReduceTasks(numberOfReducers);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputDirCount);
		
		exitCode = job.waitForCompletion(true) ? 0 : 1;
		if (exitCode != 0) 
			return exitCode;
		/* End Job 1 */
		
		/* Job 2 - Top K */
		conf.setInt("k", k);
		
		job = Job.getInstance(conf);
		job.setJobName("Lab 3 - Top K");
		job.setJarByClass(DriverBigData.class);
	
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MapperTopK.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(RecordCountWritable.class);

		job.setReducerClass(ReducerTopK.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(RecordCountWritable.class);
		
		job.setNumReduceTasks(1);	// global view
		
		FileInputFormat.addInputPath(job, outputDirCount);
		FileOutputFormat.setOutputPath(job, outputDirTopK);
		
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
