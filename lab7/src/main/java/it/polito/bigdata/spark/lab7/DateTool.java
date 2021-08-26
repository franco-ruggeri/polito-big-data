package it.polito.bigdata.spark.lab7;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTool {

	public static String dayOfTheWeek(String date) throws ParseException {
		String dayOfTheWeek;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date d = new Date();
		Calendar cal = Calendar.getInstance();

		d = format.parse(date);
		cal.setTime(d);

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

}