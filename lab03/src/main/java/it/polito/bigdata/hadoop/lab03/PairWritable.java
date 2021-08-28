package it.polito.bigdata.hadoop.lab03;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

/**
 * This class implements a personalized data type for Hadoop MapReduce applications.
 * It contains two strings and can be used both as a key and value.
 */
public class PairWritable implements WritableComparable<PairWritable> {
	private String first, second;

	public PairWritable() {}

	public PairWritable(String first, String second) {
		// store always in order, we want to consider (p1,p2) the same as (p2,p1)
		if (first.compareTo(second) <= 0) {
			this.first = first;
			this.second = second;
		} else {
			this.first = second;
			this.second = first;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeUTF(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readUTF();
	}

	@Override
	public String toString() {
		return first.toString() + "," + second.toString();
	}

	@Override
	public int compareTo(PairWritable o) {
		int result = first.compareTo(o.first);
		if (result == 0) {
			result = second.compareTo(o.second);
			return result;
		}
		return result;
	}

	// For key partitioning
	@Override
	public int hashCode() {
		return Objects.hash(first, second);
	}
}
