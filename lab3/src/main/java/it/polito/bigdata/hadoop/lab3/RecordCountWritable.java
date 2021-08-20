package it.polito.bigdata.hadoop.lab3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This class implements a personalized data type for Hadoop MapReduce applications.
 * It contains a string and an integer, allowing to count the occurrences of a record.
 */
public class RecordCountWritable implements WritableComparable<RecordCountWritable> {
	private String record;
	private int count;

	public RecordCountWritable() {}

	public RecordCountWritable(String record, int count) {
		this.record = record;
		this.count = count;
	}

	public RecordCountWritable(RecordCountWritable other) {
		this.record = other.record;
		this.count = other.count;
	}

	public String getWord() {
		return record;
	}

	public void setRecord(String record) {
		this.record = record;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public int compareTo(RecordCountWritable other) {
		int result = Integer.compare(count, other.count);
		if (result == 0)
			result = record.compareTo(other.record);
		return result;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		record = in.readUTF();
		count = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(record);
		out.writeInt(count);
	}

	@Override
	public String toString() {
		return new String(record + "\t" + count);
	}
}
