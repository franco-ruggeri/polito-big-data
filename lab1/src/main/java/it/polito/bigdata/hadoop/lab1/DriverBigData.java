package it.polito.bigdata.hadoop.lab1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Path inputPath;
		Path outputDir;
		int numberOfReducers;
		int exitCode;  
		boolean bigrams;
		
		// Parse the parameters
		numberOfReducers = Integer.parseInt(args[0]);	// Number of instances of the reducer class 
		inputPath = new Path(args[1]);					// Folder containing the input data
		outputDir = new Path(args[2]);					// Output folder
		switch (args[3]) {
		case "bigram":
			bigrams = true;
			break;
		case "word":
			bigrams = false;
			break;
		default:
			throw new IllegalArgumentException("Invalid mode: " + args[3]);
		}
		
		// Configuration
		Configuration conf = this.getConf();

		// Define a new job
		Job job = Job.getInstance(conf); 

		// Assign a name to the job
		job.setJobName("Basic MapReduce Project - WordCount example");

		// Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
		FileInputFormat.addInputPath(job, inputPath);

		// Set path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDir);

		// Specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// Set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set mapper and reducer
		if (!bigrams) {
			job.setMapperClass(MapperBigDataWord.class);
			job.setMapOutputKeyClass(Text.class);
			job.setOutputKeyClass(Text.class);
		} else {
			job.setMapperClass(MapperBigDataBigram.class);
			job.setMapOutputKeyClass(BigramWritable.class);
			job.setOutputKeyClass(BigramWritable.class);
		}
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ReducerBigData.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Set number of reducers
		job.setNumReduceTasks(numberOfReducers);

		// Execute the job and wait for completion
		if (job.waitForCompletion(true)==true)
			exitCode=0;
		else
			exitCode=1;

		return exitCode;
	}

	/** 
	 * Main of the driver
	 */
	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);
		
		System.exit(res);
	}
}