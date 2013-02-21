/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.stram;

import com.malhartech.api.DAG;
import com.malhartech.engine.GenericTestModule;
import com.malhartech.engine.TestGeneratorInputModule;
import com.malhartech.stram.PhysicalPlan.PTOperator;
import com.malhartech.stram.TupleRecorder.PortInfo;
import com.malhartech.stram.TupleRecorder.RecordInfo;
import com.malhartech.stream.StramTestSupport;
import com.malhartech.stream.StramTestSupport.WaitCondition;

import java.io.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
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

  //@Test
  public void testSomeMethod()
  {
    try {
      TupleRecorder recorder = new TupleRecorder();
      recorder.setBytesPerPartFile(4096);
      recorder.setLocalMode(true);
      recorder.setBasePath("file://" + testWorkDir.getAbsolutePath() + "/recordings");

      recorder.addInputPortInfo("ip1", "str1");
      recorder.addInputPortInfo("ip2", "str2");
      recorder.addInputPortInfo("ip3", "str3");
      recorder.addOutputPortInfo("op1", "str4");
      recorder.setup(null);

      recorder.beginWindow(1000);
      recorder.beginWindow(1000);
      recorder.beginWindow(1000);

      Tuple t1 = new Tuple();
      t1.key = "speed";
      t1.value = "5m/h";
      recorder.writeTuple(t1, "ip1");
      recorder.endWindow();
      Tuple t2 = new Tuple();
      t2.key = "speed";
      t2.value = "4m/h";
      recorder.writeTuple(t2, "ip3");
      recorder.endWindow();
      Tuple t3 = new Tuple();
      t3.key = "speed";
      t3.value = "6m/h";
      recorder.writeTuple(t3, "ip2");
      recorder.endWindow();

      recorder.beginWindow(1000);
      Tuple t4 = new Tuple();
      t4.key = "speed";
      t4.value = "2m/h";
      recorder.writeTuple(t4, "op1");
      recorder.endWindow();
      recorder.teardown();

      FileSystem fs = new LocalFileSystem();
      fs.initialize((new Path(recorder.getBasePath()).toUri()), new Configuration());
      Path path;
      FSDataInputStream is;
      String line;
      BufferedReader br;

      path = new Path(recorder.getBasePath(), TupleRecorder.INDEX_FILE);
      is = fs.open(path);
      br = new BufferedReader(new InputStreamReader(is));

      line = br.readLine();
      //    Assert.assertEquals("check index", "B:1000:T:0:part0.txt", line);
      Assert.assertEquals("check index", "F:1000:1000:T:4:25:{\"3\":1,\"1\":1,\"0\":1,\"2\":1}:part0.txt", line);

      path = new Path(recorder.getBasePath(), TupleRecorder.META_FILE);
      //fs = FileSystem.get(path.toUri(), new Configuration());
      is = fs.open(path);
      br = new BufferedReader(new InputStreamReader(is));

      ObjectMapper mapper = new ObjectMapper();
      line = br.readLine();
      Assert.assertEquals("check version", "1.0", line);
      line = br.readLine(); // RecordInfo
      RecordInfo ri = mapper.readValue(line, RecordInfo.class);
      line = br.readLine();
      PortInfo pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port1", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port1", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      line = br.readLine();
      pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port2", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port2", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      line = br.readLine();
      pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port3", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port3", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      line = br.readLine();
      pi = mapper.readValue(line, PortInfo.class);
      Assert.assertEquals("port4", recorder.getPortInfoMap().get(pi.name).id, pi.id);
      Assert.assertEquals("port4", recorder.getPortInfoMap().get(pi.name).type, pi.type);
      Assert.assertEquals("port size", recorder.getPortInfoMap().size(), 4);
      //line = br.readLine();

      path = new Path(recorder.getBasePath(), "part0.txt");
      //fs = FileSystem.get(path.toUri(), new Configuration());
      is = fs.open(path);
      br = new BufferedReader(new InputStreamReader(is));

      line = br.readLine();
      Assert.assertEquals("check part0", "B:1000", line);
      line = br.readLine();
      Assert.assertEquals("check part0 1", "T:0:31:{\"key\":\"speed\",\"value\":\"5m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 2", "T:2:31:{\"key\":\"speed\",\"value\":\"4m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 3", "T:1:31:{\"key\":\"speed\",\"value\":\"6m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 4", "T:3:31:{\"key\":\"speed\",\"value\":\"2m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 5", "E:1000", line);
    }
    catch (IOException ex) {
      Logger.getLogger(TupleRecorderTest.class.getName()).log(Level.SEVERE, null, ex);
    }

  }

  private static File testWorkDir = new File("target", TupleRecorderTest.class.getName());

  @Test
  public void testRecording() throws Exception
  {

    DAG dag = new DAG();

    dag.getAttributes().attr(DAG.STRAM_APP_PATH).set("file://" + testWorkDir.getAbsolutePath());
    dag.getAttributes().attr(DAG.STRAM_TUPLE_RECORDING_PART_FILE_SIZE).set(1024);  // 1KB per part

    TestGeneratorInputModule op1 = dag.addOperator("op1", TestGeneratorInputModule.class);
    GenericTestModule op2 = dag.addOperator("op2", GenericTestModule.class);
    GenericTestModule op3 = dag.addOperator("op3", GenericTestModule.class);
    dag.addStream("stream1", op1.outport, op2.inport1);
    dag.addStream("stream2", op2.outport1, op3.inport1);

    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.runAsync();

    final PTOperator ptOp2 = localCluster.findByLogicalNode(dag.getOperatorWrapper(op2));
    StramTestSupport.waitForActivation(localCluster, ptOp2);

    localCluster.dnmgr.startRecording(ptOp2.id, "doesNotMatter");

    WaitCondition c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        TupleRecorder tupleRecorder = localCluster.getContainer(ptOp2).getTupleRecorder(ptOp2.id);
        return (tupleRecorder != null) && (tupleRecorder.getTotalTupleCount() >= 20);
      }

    };

    StramTestSupport.awaitCompletion(c, 15000);

    TupleRecorder tupleRecorder = localCluster.getContainer(ptOp2).getTupleRecorder(ptOp2.id);
    long startTime = tupleRecorder.getStartTime();

    localCluster.dnmgr.stopRecording(ptOp2.id);
    c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        TupleRecorder tupleRecorder = localCluster.getContainer(ptOp2).getTupleRecorder(ptOp2.id);
        return (tupleRecorder == null);
      }

    };
    StramTestSupport.awaitCompletion(c, 5000);

    BufferedReader br;
    String line;
    File dir = new File(testWorkDir, "recordings/" + ptOp2.id + "/" + startTime);
    File file;

    file = new File(dir, "meta.txt");
    Assert.assertTrue("meta file should exist", file.exists());
    br = new BufferedReader(new FileReader(file));
    line = br.readLine();
    Assert.assertEquals("version should be 1.0", line, "1.0");
    line = br.readLine();
    Assert.assertTrue("should contain start time", line.contains("\"startTime\""));
    line = br.readLine();
    Assert.assertTrue("should contain name, streamName, type and id", line.contains("\"name\"") && line.contains("\"streamName\"") && line.contains("\"type\"") && line.contains("\"id\""));
    line = br.readLine();
    Assert.assertTrue("should contain name, streamName, type and id", line.contains("\"name\"") && line.contains("\"streamName\"") && line.contains("\"type\"") && line.contains("\"id\""));

    file = new File(dir, "index.txt");
    Assert.assertTrue("index file should exist", file.exists());
    br = new BufferedReader(new FileReader(file));
    line = br.readLine();
    Assert.assertTrue("index file line should start with F:", line.startsWith("F:"));
    Assert.assertTrue("index file line should end with :part0.txt", line.endsWith(":part0.txt"));
    line = br.readLine();
    Assert.assertTrue("index file line should start with F:", line.startsWith("F:"));
    Assert.assertTrue("index file line should end with :part1.txt", line.endsWith(":part1.txt"));

    int tupleCount0 = 0;
    int tupleCount1 = 0;
    boolean beginWindowExists = false;
    boolean endWindowExists = false;

    String[] partFiles = {"part0.txt", "part1.txt"};

    for (String partFile: partFiles) {
      file = new File(dir, partFile);
      if (partFile == "part0.txt") {
        Assert.assertTrue("part0.txt should be greater than 1KB", file.length() >= 1024);
      }
      Assert.assertTrue(partFile + " should exist", file.exists());
      br = new BufferedReader(new FileReader(file));
      while ((line = br.readLine()) != null) {
        if (line.startsWith("B:")) {
          beginWindowExists = true;
        }
        else if (line.startsWith("E:")) {
          endWindowExists = true;
        }
        else if (line.startsWith("T:0:")) {
          tupleCount0++;
        }
        else if (line.startsWith("T:1")) {
          tupleCount1++;
        }
      }
    }
    Assert.assertTrue("begin window should exist", beginWindowExists);
    Assert.assertTrue("end window should exist", endWindowExists);
    Assert.assertTrue("tuple exists for port 0", tupleCount0 > 0);
    Assert.assertTrue("tuple exists for port 1", tupleCount1 > 0);
    Assert.assertTrue("total tuple count >= 20", tupleCount0 + tupleCount1 >= 20);
    localCluster.shutdown();
  }

}