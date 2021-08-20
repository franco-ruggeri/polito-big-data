package it.polito.bigdata.hadoop.lab3;

import java.util.Vector;

/**
 * This class encapsulates a container that keeps only the top K elements. 
 * The ranking is determined by the natural order, available because the elements implement the Comparable interface.
 */
public class TopKVector<T extends Comparable<T>> {
	private Vector<T> topK;
	private Integer k;

	public TopKVector(int k) {
		this.topK = new Vector<>();
		this.k = k;
	}

	public int getK() {
		return this.k;
	}

	public Vector<T> getTopK() {
		return new Vector<>(topK);		// return copy
	}

	public void update(T element) {
		if (topK.size() < k) {
			topK.addElement(element);
			sortAfterInsert();
		} else {
			if (element.compareTo(topK.elementAt(k - 1)) > 0) {
				topK.setElementAt(element, k - 1);
				sortAfterInsert();
			}
		}
	}

	private void sortAfterInsert() {
		// the last object is the only one potentially not in the right position
		T swap;
		for (int pos = topK.size() - 1; pos > 0 && topK.elementAt(pos).compareTo(topK.elementAt(pos - 1)) > 0; pos--) {
			swap = topK.elementAt(pos);
			topK.setElementAt(topK.elementAt(pos - 1), pos);
			topK.setElementAt(swap, pos - 1);
		}
	}
}
