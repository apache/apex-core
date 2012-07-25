/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.stream;

import static org.junit.Assert.assertTrue;

import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.Buffer.EndWindow;
import com.malhartech.dag.EndWindowTuple;
import com.malhartech.dag.StreamContext;
import com.malhartech.dag.Tuple;

/**
 * Bunch of utilities shared between tests.
 */
abstract public class StramTestSupport {

  static Tuple generateTuple(Object payload, long windowId, StreamContext sc) {
    Tuple t = new Tuple(payload);
    t.setWindowId(windowId);
    t.setType(DataType.SIMPLE_DATA);
    t.setContext(sc);
    return t;
  }
  
  static Tuple generateBeginWindowTuple(String nodeid, long windowId, StreamContext sc)
  {
    Tuple t = new Tuple(null);
    t.setType(DataType.BEGIN_WINDOW);
    t.setWindowId(windowId);
    t.setContext(sc);
    
    return t;
  }
  
  
  static Tuple generateEndWindowTuple(String nodeid, long windowId, int tupleCount, StreamContext sc)
  {
    EndWindow.Builder ewb = EndWindow.newBuilder();
    ewb.setNode(nodeid);
    ewb.setTupleCount(tupleCount);
     
    Data.Builder db = Data.newBuilder();
    db.setType(DataType.END_WINDOW);
    db.setWindowId(windowId);
    db.setEndwindow(ewb);
    
    //Data data = db.build();
    EndWindowTuple t = new EndWindowTuple();
    t.setTupleCount(tupleCount);
    t.setWindowId(windowId);
    t.setContext(sc);
    return t;
  }
  

  public static void checkStringMatch(String print, String expected, String got) {
    assertTrue(
        print + " doesn't match, got: " + got + " expected: " + expected,
        got.matches(expected));
  }  
  
}
