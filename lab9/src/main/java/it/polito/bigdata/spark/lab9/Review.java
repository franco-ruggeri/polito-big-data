package it.polito.bigdata.spark.lab9;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Review implements Serializable {
	private int score, textLength, nWords;
	private String text;
	private double label;
	
	public Review() {}
	
	public Review(int score, String text, double label) {
		this.score = score;
		this.text = text;
		this.label = label;
		this.textLength = text.length();
		this.nWords = text.split("\\s+").length;
	}
	
	public int getScore() {
		return score;
	}
	
	public void setScore(int score) {
		this.score = score;
	}
	
	public int getTextLength() {
		return textLength;
	}

	public void setTextLength(int textLength) {
		this.textLength = textLength;
	}

	public int getnWords() {
		return nWords;
	}

	public void setnWords(int nWords) {
		this.nWords = nWords;
	}

	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}
	
	public double getLabel() {
		return label;
	}
	
	public void setLabel(double helpfulness) {
		this.label = helpfulness;
	}
}
