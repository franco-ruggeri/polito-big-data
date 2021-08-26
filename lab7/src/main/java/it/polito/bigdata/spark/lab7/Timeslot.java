package it.polito.bigdata.spark.lab7;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Objects;

@SuppressWarnings("serial")
public class Timeslot implements Serializable, Comparable<Timeslot> {
	private String dayOfTheWeek;
	private int hour;
	
	public Timeslot(String timestamp) throws ParseException {
		String[] fields = timestamp.split(" ");
		this.dayOfTheWeek = DateTool.dayOfTheWeek(fields[0]);
		this.hour = Integer.parseInt(fields[1].split(":")[0]);
	}
	
	public String getDayOfTheWeek() {
		return dayOfTheWeek;
	}

	public void setDayOfTheWeek(String dayOfTheWeek) {
		this.dayOfTheWeek = dayOfTheWeek;
	}

	public int getHour() {
		return hour;
	}

	public void setHour(int hour) {
		this.hour = hour;
	}

	@Override
	public int hashCode() {
		return Objects.hash(dayOfTheWeek, hour);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Timeslot))
			return false;
		Timeslot other = (Timeslot) o;
		return dayOfTheWeek.equals(other.dayOfTheWeek) && hour == other.hour;
	}
	
	@Override
	public int compareTo(Timeslot o) {
		int result = Integer.compare(o.hour, hour);
		if (result == 0)
			result = o.dayOfTheWeek.compareTo(dayOfTheWeek);
		return result;
	}
}
