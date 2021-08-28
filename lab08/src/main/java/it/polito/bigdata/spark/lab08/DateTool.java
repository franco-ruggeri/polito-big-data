package it.polito.bigdata.spark.lab08;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTool {
	
	public static String dayOfTheWeek(Date date) {
		String dayOfTheWeek;
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);

		switch (cal.get(Calendar.DAY_OF_WEEK)) {
		case Calendar.SUNDAY:
			dayOfTheWeek = "Sun";
			break;
		case Calendar.MONDAY:
			dayOfTheWeek = "Mon";
			break;
		case Calendar.TUESDAY:
			dayOfTheWeek = "Tue";
			break;
		case Calendar.WEDNESDAY:
			dayOfTheWeek = "Wed";
			break;
		case Calendar.THURSDAY:
			dayOfTheWeek = "Thu";
			break;
		case Calendar.FRIDAY:
			dayOfTheWeek = "Fri";
			break;
		case Calendar.SATURDAY:
			dayOfTheWeek = "Sat";
			break;
		default:
			throw new RuntimeException("Unexpected switch case for day of the week");
		}

		return dayOfTheWeek;
	}

	public static String dayOfTheWeek(Timestamp date) {
		return dayOfTheWeek(new Date(date.getTime()));
	}

	public static String dayOfTheWeek(String date) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		return dayOfTheWeek(format.parse(date));
	}

	public static int hour(Timestamp date) {
		Date d = new Date(date.getTime());
		Calendar cal = Calendar.getInstance();
		cal.setTime(d);
		return cal.get(Calendar.HOUR_OF_DAY);
	}

}