package it.polito.bigdata.hadoop.lab2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program - Driver
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// parse the parameters
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		String filter = args[2];
		boolean bigram;
		switch (args[3]) {
		case "bigram":
			bigram = true;
			break;
		case "word":
			bigram = false;
			break;
		default:
			throw new IllegalArgumentException("Invalid mode: " + args[3]);
		}

		// configuration
		Configuration conf = this.getConf();
		conf.set("filter", filter);
		conf.setBoolean("bigram", bigram);

		// define a new job
		Job job = Job.getInstance(conf); 

		// assign a name to the job
		job.setJobName("Lab 2 - Filter");

		// set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
		FileInputFormat.addInputPath(job, inputPath);

		// set path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDir);

		// specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// set job input format
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// set map class
		job.setMapperClass(MapperBigData.class);

		// set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// set number of reducers (map-only job)
		job.setNumReduceTasks(0);

		// execute the job and wait for completion
		int exitCode = job.waitForCompletion(true) ? 0 : 1;

		return exitCode;

	}


	/** 
	 * Main of the driver
	 */
	public static void main(String args[]) throws Exception {
		// exploit the ToolRunner class to "configure" and run the Hadoop application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}
