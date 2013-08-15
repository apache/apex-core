package com.datatorrent.mapreduce;


import org.junit.Test;

import com.datatorrent.api.LocalMode;

public class MRLegacyDebuggerApplicationTest {

	@Test
	public void testSomeMethod() throws Exception {
		LocalMode.runApp(new MRLegacyDebuggerApplication(), 100000);
	}

}
