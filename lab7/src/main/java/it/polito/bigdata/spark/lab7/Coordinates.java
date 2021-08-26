package it.polito.bigdata.spark.lab7;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Coordinates implements Serializable {
	private double longitude, latitude;
	
	public Coordinates(double longitude, double latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}
	
	@Override
	public String toString() {
		return "" + longitude + "," + latitude;
	}
}
