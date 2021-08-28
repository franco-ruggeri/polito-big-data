package it.polito.bigdata.hadoop.lab02;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapReduce program - Mapper
 */
class MapperBigData extends Mapper<Text, Text, Text, IntWritable> {
	private String filter;
	private boolean bigram;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		// get configuration
		Configuration config = context.getConfiguration();
		filter = config.get("filter");
		bigram = config.getBoolean("bigram", false);
	}

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String strValue = value.toString();

		// check value format
		IntWritable intValue;
		try {
			intValue = new IntWritable(Integer.parseInt(strValue));
		} catch (NumberFormatException e) {
			System.err.println("Invalid line: value = " + strValue);
			return;
		}

		/*
		 * Filter:
		 * - Bigram: filter indicates the full word the bigram has to contain
		 * - Word: filter indicates beginning of the word
		 */
		if (!bigram) {
			if (key.toString().startsWith(filter))
				context.write(key, intValue);
		} else {
			BigramWritable bigram;
			try {
				bigram = new BigramWritable(key);
			} catch (IllegalArgumentException e) {
				return;
			}
			if (bigram.contains(filter))
				context.write(key, intValue);
		}
	}
}
