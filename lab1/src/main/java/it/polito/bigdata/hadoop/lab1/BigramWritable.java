package it.polito.bigdata.hadoop.lab1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class BigramWritable implements WritableComparable<BigramWritable> {
	private String first, second;
	private static String regexAlpha = "[a-z0-9]+";

	// For deserialization
	public BigramWritable() {}

	public BigramWritable(String first, String second) {
		this.first = first;
		this.second = second;
	}

	// Serialization (to network)
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeUTF(second);
	}

	// Deserialization (from network)
	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readUTF();
	}

	// For output on text files
	@Override
	public String toString() {
		return first.toString() + " " + second.toString();
	}

	// For shuffle and sort phase
	@Override
	public int compareTo(BigramWritable o) {
		int result = first.compareTo(o.first);
		if (result == 0)
			result = second.compareTo(o.second);
		return result;
	}

	// For key partitioning
	@Override
	public int hashCode() {
		return Objects.hash(first, second);
	}

	public boolean isAlphanumeric() {
		return first.matches(regexAlpha) && second.matches(regexAlpha);
	}
}
