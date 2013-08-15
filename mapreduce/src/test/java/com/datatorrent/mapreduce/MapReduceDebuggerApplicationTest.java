package com.datatorrent.mapreduce;

import org.junit.Test;

import com.datatorrent.api.LocalMode;

public class MapReduceDebuggerApplicationTest {

	@Test
	public void testSomeMethod() throws Exception {
		LocalMode.runApp(new MapReduceDebuggerApplication(), 100000);
	}


}
