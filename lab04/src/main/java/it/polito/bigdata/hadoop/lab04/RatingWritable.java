package it.polito.bigdata.hadoop.lab04;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * This class implements a personalized data type for Hadoop MapReduce applications.
 * It can store a rating for a product.
 */
public class RatingWritable implements Writable {
	String productId;
	double score;

	public RatingWritable() {}
	
	public RatingWritable(String productId, double score) {
		this.productId = productId;
		this.score = score;
	}
	
	public RatingWritable(RatingWritable other) {
		this.productId = other.productId;
		this.score = other.score;
	}
	
	String getProductId() {
		return productId;
	}
	
	double getScore() {
		return score;
	}
	
	public void normalizeScore(double userAverageRating) {
		score -= userAverageRating;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(productId);
		out.writeDouble(score);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		productId = in.readUTF();
		score = in.readDouble();
	}
	
	@Override
	public String toString() {
		return productId + "\t" + score;
	}
}
