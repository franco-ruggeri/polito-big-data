package it.polito.bigdata.spark.lab07;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPathRegister = args[0];
		String inputPathStations = args[1];
		String outputPath = args[2];
		double threshold = Double.parseDouble(args[3]);
		
		SparkConf conf = new SparkConf().setAppName("Lab 7 - Bike sharing data analysis with Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaPairRDD<Tuple2<Integer,Timeslot>,Double> criticalities = context.textFile(inputPathRegister)
				// filter out header wrong lines
				.filter(line -> {
					if (line.startsWith("station\t"))
						return false;
					String[] fields = line.split("\t");
					int usedSlots = Integer.parseInt(fields[2]);
					int freeSlots = Integer.parseInt(fields[3]);
					return usedSlots > 0 || freeSlots > 0;
				})
				// critical: (stationId, timeslot) -> (+1, +1)
				// non-critical: (stationId, timeslot) -> (0, +1)
				.mapToPair(line -> {
					String[] fields = line.split("\\t");
					
					int stationId = Integer.parseInt(fields[0]);
					String timestamp = fields[1];
					Timeslot timeslot = new Timeslot(timestamp);
					Tuple2<Integer,Timeslot> stationTimeslot = new Tuple2<>(stationId, timeslot);
					
					int freeSlots = Integer.parseInt(fields[3]);
					int nCritical = freeSlots == 0 ? 1 : 0;
					int nTotal = 1;
					Tuple2<Integer,Integer> counts = new Tuple2<>(nCritical, nTotal);
					
					return new Tuple2<>(stationTimeslot, counts);
				})
				// (stationId, timeslot) -> (nCritical, nTotal)
				.reduceByKey((c1, c2) -> new Tuple2<>(c1._1 + c2._1, c1._2 + c2._2))
				// (stationId, timeslot) -> criticality
				.mapValues(c -> c._1 / (double) c._2)
				// filter out records below the criticality threshold
				.filter(pair -> Double.compare(pair._2, threshold) >= 0);
		
		JavaPairRDD<Integer,Tuple2<Timeslot,Double>> mostCriticalTimeslots = criticalities
				// stationId -> (timeslot, criticality)
				.mapToPair(pair -> {
					Integer stationId = pair._1._1;
					Timeslot timeslot = pair._1._2;
					Double criticality = pair._2;
					Tuple2<Timeslot,Double> timeslotCriticality = new Tuple2<>(timeslot, criticality);
					return new Tuple2<>(stationId, timeslotCriticality);
				})
				// select timeslot with max criticality for each stationId
				.reduceByKey((pair1, pair2) -> {
					int result = pair1._2.compareTo(pair2._2);
					if (result == 0)
						result = pair1._1.compareTo(pair2._1);
					return result > 0 ? pair1 : pair2;
				});
		
		Map<Integer,Coordinates> stationsCoordinates = context.textFile(inputPathStations)
				// filter out header and wrong lines
				.filter(line -> {
					if (line.startsWith("id\t"))
						return false;
					try {
						String[] fields = line.split("\\t");
						Integer.parseInt(fields[0]);
						Double.parseDouble(fields[1]);
						Double.parseDouble(fields[2]);
						return true;
					} catch (NumberFormatException e) {
						return false;
					}
				})
				// stationId -> coordinates
				.mapToPair(line -> {
					String[] fields = line.split("\\t");
					int stationId = Integer.parseInt(fields[0]);
					double longitude = Double.parseDouble(fields[1]);
					double latitude = Double.parseDouble(fields[2]);
					Coordinates stationCoordinates = new Coordinates(longitude, latitude);
					return new Tuple2<>(stationId, stationCoordinates);
				})
				// ~3000 stations fit main memory
				.collectAsMap();
		
		// broadcast because it is a large variable
		// important: a Map cannot be broadcasted, because it is not Serializable, must convert to HashMap
		Broadcast<HashMap<Integer,Coordinates>> coordinatesBroadcast = context.broadcast(new HashMap<>(stationsCoordinates));
		
		// map to KML format
		JavaRDD<String> resultKML = mostCriticalTimeslots
				.map(pair -> {
					int stationId = pair._1;
					Timeslot timeslot = pair._2._1;
					double criticality = pair._2._2;
					Coordinates stationCoordinates = coordinatesBroadcast.value().get(stationId);
					
					Map<String,Object> extendedData = new HashMap<>();
					extendedData.put("DayWeek", timeslot.getDayOfTheWeek());
					extendedData.put("Hour", timeslot.getHour());
					extendedData.put("Criticality", criticality);
					
					return KML.format(Integer.toString(stationId), extendedData, stationCoordinates);
				});
		
		// store data in one single output file (number of partitions reduced to 1)
		resultKML.coalesce(1).saveAsTextFile(outputPath); 

		context.close();
	}
}
