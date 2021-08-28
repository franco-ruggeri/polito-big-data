package it.polito.bigdata.hadoop.lab04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class implements a personalized data type for Hadoop MapReduce applications.
 * It can store sum and count, useful to compute the average.
 */
public class AverageWritable implements Writable {
	double sum;
	int count;

	public AverageWritable() {}
	
	public AverageWritable(double sum, int count) {
		this.sum = sum;
		this.count = count;
	}
	
	public double getSum() {
		return sum;
	}
	
	public int getCount() {
		return count;
	}
	
	public double getAverage() {
		return sum / count;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(sum);
		out.writeInt(count);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sum = in.readDouble();
		count = in.readInt();
	}
	
	@Override
	public String toString() {
		return "" + getAverage();
	}
}
