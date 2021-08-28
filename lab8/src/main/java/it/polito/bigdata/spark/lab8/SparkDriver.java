package it.polito.bigdata.spark.lab8;

import java.sql.Timestamp;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.DataTypes;

public class SparkDriver {

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		String inputPathRegister = args[0];
		String inputPathStations = args[1];
		String outputPath = args[2];
		double threshold = Double.parseDouble(args[3]);

		SparkSession session = SparkSession.builder().appName("Lab 8 - Bike sharing data anlysis with Spark SQL").getOrCreate();

		// UDFs
		UDFRegistration udfRegistration = session.udf();
		udfRegistration.register("critical", (Integer n) -> n == 0 ? 1 : 0, DataTypes.IntegerType);
		udfRegistration.register("mydayofweek", (Timestamp timestamp) -> DateTool.dayOfTheWeek(timestamp), DataTypes.StringType);
		
		// load datasets
		DataFrameReader reader = session.read()
				.option("header", true)
				.option("inferSchema", true)
				.option("sep", "\t");
		Dataset<Row> register = reader.csv(inputPathRegister);
		Dataset<Row> stations = reader.csv(inputPathStations);
		
		// check schema
		register.printSchema();
		stations.printSchema();
		
		// create views for query
		register.createOrReplaceTempView("register");
		stations.createOrReplaceTempView("station");
		
		// query
		Dataset<Row> criticalStationTimeslots = session.sql(
				"SELECT station, MYDAYOFWEEK(timestamp) as dayOfWeek, " + 
						"HOUR(timestamp) as hour, AVG(CRITICAL(free_slots)) as criticality, " +
						"longitude as stationLongitude, latitude as stationLatitude " +
				"FROM register, station " +
				"WHERE register.station = station.id AND (used_slots <> 0 OR free_slots <> 0) " +
				"GROUP BY station, dayOfWeek, hour, longitude, latitude " +
				"HAVING AVG(CRITICAL(free_slots)) > " + threshold + " " +
				"ORDER BY criticality DESC, station, dayOfWeek, hour");
		
		// write CSV
		criticalStationTimeslots.write().option("header", true).csv(outputPath);
		
		session.close();
	}
}
