/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.stram.TupleRecorder.PortInfo;
import com.malhartech.stram.TupleRecorder.RecordInfo;
import java.io.IOException;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class TupleRecorderTest
{
  public TupleRecorderTest()
  {
  }

  public class Tuple
  {
    public String key;
    public String value;
  }

  @Test
  public void testSomeMethod()
  {
    try {
      TupleRecorder recorder = new TupleRecorder();
      recorder.setBytesPerFile(4096);

      recorder.addInputPortInfo("ip1", "str1");
      recorder.addInputPortInfo("ip2", "str2");
      recorder.addInputPortInfo("ip3", "str3");
      recorder.addOutputPortInfo("op1", "str4");

      recorder.setBasePath("hdfs://localhost:9000/yahoofinance/");
      recorder.setup(null);

      recorder.beginWindow(1000);
      Tuple t1 = new Tuple();
      t1.key = "speed";
      t1.value = "5m/h";
      recorder.writeTuple(t1, "ip1");
      Tuple t2 = new Tuple();
      t2.key = "speed";
      t2.value = "4m/h";
      recorder.writeTuple(t2, "ip3");
      Tuple t3 = new Tuple();
      t3.key = "speed";
      t3.value = "6m/h";
      recorder.writeTuple(t3, "ip2");
      Tuple t4 = new Tuple();
      t4.key = "speed";
      t4.value = "2m/h";
      recorder.writeTuple(t4, "op1");
      recorder.endWindow();
      recorder.teardown();

      FileSystem fs;
      Path path;
      FSDataInputStream is;
      String line;
      path = new Path(recorder.getBasePath(), TupleRecorder.INDEX_FILE);
      fs = FileSystem.get(path.toUri(), new Configuration());
      is = fs.open(path);
      line = is.readLine();
      //    Assert.assertEquals("check index", "B:1000:T:0:part0.txt", line);
      Assert.assertEquals("check index", "B:1000:T:0:" + recorder.getBasePath() + "part0.txt", line);

      path = new Path(recorder.getBasePath(), TupleRecorder.META_FILE);
      fs = FileSystem.get(path.toUri(), new Configuration());
      is = fs.open(path);

      ObjectMapper mapper = new ObjectMapper();
      line = is.readLine();
      Assert.assertEquals("check version", "1.0", line);
      line = is.readLine();
      PortInfo pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port1", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port1", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      line = is.readLine();
      pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port2", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port2", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      line = is.readLine();
      pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port3", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port3", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      line = is.readLine();
      pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port4", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port4", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      Assert.assertEquals("port size", recorder.getPortInfoMap().size(), 4);
      line = is.readLine();

      path = new Path(recorder.getBasePath(), "part0.txt");
      fs = FileSystem.get(path.toUri(), new Configuration());
      is = fs.open(path);
      line = is.readLine();
      Assert.assertEquals("check part0", "B:1000", line);
      line = is.readLine();
      Assert.assertEquals("check part0 1", "T:0:31:{\"key\":\"speed\",\"value\":\"5m/h\"}", line);
      line = is.readLine();
      Assert.assertEquals("check part0 2", "T:2:31:{\"key\":\"speed\",\"value\":\"4m/h\"}", line);
      line = is.readLine();
      Assert.assertEquals("check part0 3", "T:1:31:{\"key\":\"speed\",\"value\":\"6m/h\"}", line);
      line = is.readLine();
      Assert.assertEquals("check part0 4", "T:3:31:{\"key\":\"speed\",\"value\":\"2m/h\"}", line);
      line = is.readLine();
      Assert.assertEquals("check part0 5", "E:1000", line);
    }
    catch (IOException ex) {
      ex.printStackTrace();
    }

  }

}