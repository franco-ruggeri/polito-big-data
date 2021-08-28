package it.polito.bigdata.spark.lab06;

import java.io.Serializable;
import java.util.Objects;

@SuppressWarnings("serial")
public class ProductPair implements Serializable {
	private String product1, product2;
	
	public ProductPair(String product1, String product2) {
		// store always in order, we want to consider (p1,p2) the same as (p2,p1)
		if (product1.compareTo(product2) <= 0) {
			this.product1 = product1;
			this.product2 = product2;
		} else {
			this.product1 = product2;
			this.product2 = product1;
		}
	}
	
	@Override
	public int hashCode() {
		return Objects.hash(product1, product2);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof ProductPair))
			return false;
		ProductPair other = (ProductPair) o;
		return product1.equals(other.product1) && product2.equals(other.product2);
	}

	@Override
	public String toString() {
		return product1 + " " + product2;
	}
}
