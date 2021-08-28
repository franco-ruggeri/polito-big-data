package it.polito.bigdata.spark.lab07;

import java.util.Map;

public class KML {

	public static String format(String name, Map<String,?> extendedData, Coordinates coordinates) {
		StringBuffer sb = new StringBuffer();
		
		sb.append("<Placemark>");
		sb.append("<name>").append(name).append("</name>");
		
		sb.append("<ExtendedData>");
		extendedData.forEach((k, v) -> {
			sb.append("<Data name=\"").append(k).append("\">");
			sb.append("<value>").append(v).append("</value>");
			sb.append("</Data>");
		});
		sb.append("</ExtendedData>");
		
		sb.append("<Point><coordinates>").append(coordinates).append("</coordinates></Point>");
		sb.append("</Placemark>");
		
		return sb.toString();
	}
	
}
