package it.polito.bigdata.hadoop.lab3;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import it.polito.bigdata.hadoop.lab03.RecordCountWritable;
import it.polito.bigdata.hadoop.lab03.TopKVector;

/**
 * Unit tests for TopKVector class
 */
class TestTopKVector {
	TopKVector<RecordCountWritable> top3;
	List<RecordCountWritable> elements;

	@BeforeEach
	void setUp() throws Exception {
		top3 = new TopKVector<>(3);
		
		elements = Arrays.asList(
				new RecordCountWritable("p1,p2", 4),
				new RecordCountWritable("p1,p3", 40),
				new RecordCountWritable("p2,p4", 3),
				new RecordCountWritable("p5,p6", 6),
				new RecordCountWritable("p15,p16", 1));
	}

	@AfterEach
	void tearDown() throws Exception {}

	@Test
	void testInstructions() {
		elements.forEach(e -> top3.update(e));
		List<String> top3Str = top3.getTopK().stream().map(RecordCountWritable::toString).collect(Collectors.toList());
		List<String> correct = Arrays.asList(
				"p1,p3\t40",
				"p5,p6\t6",
				"p1,p2\t4");
		assertTrue(top3Str.equals(correct));
	}
}