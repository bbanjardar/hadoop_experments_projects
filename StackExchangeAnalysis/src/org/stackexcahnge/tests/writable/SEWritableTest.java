package org.stackexcahnge.tests.writable;

import static org.junit.Assert.*;

import org.junit.Test;
import org.stackexcahnge.writable.SEWritable;


public class SEWritableTest {

	@Test
	public void testConstructorQuestion() {
		
		String value = "1,,2011-03-13T19:49:22.470,0,What's the difference?";
		SEWritable seWritable = new SEWritable(value);		
		assertEquals(1, seWritable.getPostType());
		assertEquals(0,seWritable.getCommentsCount());
		assertEquals("2011-03-13T19:49:22.470", seWritable.getDateCreatedUpdated());
		assertEquals(",What's the difference?", seWritable.getPostText());
	}
	
	@Test
	public void testConstructorAnswer() {
		
		String value = "2,6,2011-03-13T19:49:22.470,4,Squats, deadlifts, and bench";
		SEWritable seWritable = new SEWritable(value);		
		assertEquals(6, seWritable.getParentId());
		assertEquals(2, seWritable.getPostType());
		assertEquals(4,seWritable.getCommentsCount());
		assertEquals("2011-03-13T19:49:22.470", seWritable.getDateCreatedUpdated());
		assertEquals(",Squats, deadlifts, and bench", seWritable.getPostText());
	}

}
