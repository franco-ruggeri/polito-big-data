package it.polito.bigdata.spark.lab06;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

@SuppressWarnings("serial")
public class FrequencyComparator implements Comparator<Tuple2<Integer,ProductPair>>, Serializable {

	@Override
	public int compare(Tuple2<Integer, ProductPair> pair1, Tuple2<Integer, ProductPair> pair2) {
		return pair1._1.compareTo(pair2._1);
	}
	
}
