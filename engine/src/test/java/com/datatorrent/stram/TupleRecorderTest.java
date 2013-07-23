/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.stram;

import com.datatorrent.engine.GenericTestOperator;
import com.datatorrent.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.PhysicalPlan.PTOperator;
import com.datatorrent.stram.TupleRecorder.PortInfo;
import com.datatorrent.stram.TupleRecorder.RecordInfo;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;
import com.datatorrent.stram.util.HdfsPartFileCollection;
import java.io.*;
import java.util.ArrayList;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@datatorrent.com>
 */
public class TupleRecorderTest
{
  @Before
  public void setup() throws IOException
  {
    StramChild.eventloop.start();
  }

  @After
  public void teardown()
  {
    StramChild.eventloop.stop();
  }

  public TupleRecorderTest()
  {
  }

  public class Tuple
  {
    public String key;
    public String value;
  }

  @Test
  public void testRecorder()
  {
    try {
      TupleRecorder recorder = new TupleRecorder();
      recorder.getStorage().setBytesPerPartFile(4096);
      recorder.getStorage().setLocalMode(true);
      recorder.getStorage().setBasePath("file://" + testWorkDir.getAbsolutePath() + "/recordings");

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
      fs.initialize((new Path(recorder.getStorage().getBasePath()).toUri()), new Configuration());
      Path path;
      FSDataInputStream is;
      String line;
      BufferedReader br;

      path = new Path(recorder.getStorage().getBasePath(), HdfsPartFileCollection.INDEX_FILE);
      is = fs.open(path);
      br = new BufferedReader(new InputStreamReader(is));

      line = br.readLine();
      //    Assert.assertEquals("check index", "B:1000:T:0:part0.txt", line);
      Assert.assertTrue("check index", line.matches("F:part0.txt:\\d+-\\d+:4:T:1000-1000:33:\\{\"3\":\"1\",\"1\":\"1\",\"0\":\"1\",\"2\":\"1\"\\}"));

      path = new Path(recorder.getStorage().getBasePath(), HdfsPartFileCollection.META_FILE);
      //fs = FileSystem.get(path.toUri(), new Configuration());
      is = fs.open(path);
      br = new BufferedReader(new InputStreamReader(is));

      ObjectMapper mapper = new ObjectMapper();
      line = br.readLine();
      Assert.assertEquals("check version", "1.1", line);
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

      path = new Path(recorder.getStorage().getBasePath(), "part0.txt");
      //fs = FileSystem.get(path.toUri(), new Configuration());
      is = fs.open(path);
      br = new BufferedReader(new InputStreamReader(is));

      line = br.readLine();
      Assert.assertEquals("check part0", "B:1000", line);
      line = br.readLine();
      Assert.assertEquals("check part0 1", "T:0:30:{\"key\":\"speed\",\"value\":\"5m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 2", "T:2:30:{\"key\":\"speed\",\"value\":\"4m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 3", "T:1:30:{\"key\":\"speed\",\"value\":\"6m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 4", "T:3:30:{\"key\":\"speed\",\"value\":\"2m/h\"}", line);
      line = br.readLine();
      Assert.assertEquals("check part0 5", "E:1000", line);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

  }

  private static File testWorkDir = new File("target", TupleRecorderTest.class.getName());
  private static int testTupleCount = 100;

  @Test
  public void testRecordingFlow() throws Exception
  {

    LogicalPlan dag = new LogicalPlan();

    dag.getAttributes().attr(LogicalPlan.APPLICATION_PATH).set("file://" + testWorkDir.getAbsolutePath());
    dag.getAttributes().attr(LogicalPlan.TUPLE_RECORDING_PART_FILE_SIZE).set(1024);  // 1KB per part

    TestGeneratorInputOperator op1 = dag.addOperator("op1", TestGeneratorInputOperator.class);
    GenericTestOperator op2 = dag.addOperator("op2", GenericTestOperator.class);
    GenericTestOperator op3 = dag.addOperator("op3", GenericTestOperator.class);

    op1.setEmitInterval(100); // emit every 100 msec
    dag.addStream("stream1", op1.outport, op2.inport1);//.setInline(true);
    dag.addStream("stream2", op2.outport1, op3.inport1);//.setInline(true);

    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.runAsync();

    final PTOperator ptOp2 = localCluster.findByLogicalNode(dag.getMeta(op2));
    StramTestSupport.waitForActivation(localCluster, ptOp2);

    testRecordingOnOperator(localCluster, ptOp2, 2);

    final PTOperator ptOp1 = localCluster.findByLogicalNode(dag.getMeta(op1));
    StramTestSupport.waitForActivation(localCluster, ptOp1);

    testRecordingOnOperator(localCluster, ptOp1, 1);

    localCluster.shutdown();
  }

  private void testRecordingOnOperator(final StramLocalCluster localCluster, final PTOperator op, int numPorts) throws Exception
  {
    localCluster.dnmgr.startRecording(op.getId(), null);

    WaitCondition c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        TupleRecorder tupleRecorder = localCluster.getContainer(op).getTupleRecorder(op.getId(), null);
        return tupleRecorder != null;
      }

    };
    Assert.assertTrue("Should get a tuple recorder within 2 seconds", StramTestSupport.awaitCompletion(c, 2000));
    TupleRecorder tupleRecorder = localCluster.getContainer(op).getTupleRecorder(op.getId(), null);
    long startTime = tupleRecorder.getStartTime();
    BufferedReader br;
    String line;
    File dir = new File(testWorkDir, "recordings/" + op.getId() + "/" + startTime);
    File file;

    file = new File(dir, "meta.txt");
    Assert.assertTrue("meta file should exist", file.exists());
    br = new BufferedReader(new FileReader(file));
    line = br.readLine();
    Assert.assertEquals("version should be 1.1", line, "1.1");
    line = br.readLine();
    Assert.assertTrue("should contain start time", line != null && line.contains("\"startTime\""));
    for (int i = 0; i < numPorts; i++) {
      line = br.readLine();
      Assert.assertTrue("should contain name, streamName, type and id", line != null && line.contains("\"name\"") && line.contains("\"streamName\"") && line.contains("\"type\"") && line.contains("\"id\""));
    }

    c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        TupleRecorder tupleRecorder = localCluster.getContainer(op).getTupleRecorder(op.getId(), null);
        return (tupleRecorder.getTotalTupleCount() >= testTupleCount);
      }

    };

    Assert.assertTrue("Should record more than " + testTupleCount + " tuples within 15 seconds", StramTestSupport.awaitCompletion(c, 15000));

    localCluster.dnmgr.stopRecording(op.getId(), null);
    c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        TupleRecorder tupleRecorder = localCluster.getContainer(op).getTupleRecorder(op.getId(), null);
        return (tupleRecorder == null);
      }

    };
    Assert.assertTrue("Tuple recorder shouldn't exist any more after stopping", StramTestSupport.awaitCompletion(c, 5000));

    file = new File(dir, "index.txt");
    Assert.assertTrue("index file should exist", file.exists());
    br = new BufferedReader(new FileReader(file));

    ArrayList<String> partFiles = new ArrayList<String>();
    int indexCount = 0;
    while ((line = br.readLine()) != null) {
      String partFile = "part" + indexCount + ".txt";
      if (line.startsWith("F:" + partFile + ":")) {
        partFiles.add(partFile);
        indexCount++;
      }
      else if (line.startsWith("E")) {
        Assert.assertEquals("index file should end after E line", br.readLine(), null);
        break;
      }
      else {
        Assert.fail("index file line is not starting with F or E");
      }
    }

    int tupleCount[] = new int[numPorts];
    boolean beginWindowExists = false;
    boolean endWindowExists = false;

    for (String partFile : partFiles) {
      file = new File(dir, partFile);
      if (!partFile.equals(partFiles.get(partFiles.size() - 1))) {
        Assert.assertTrue(partFile + " should be greater than 1KB", file.length() >= 1024);
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
        else if (line.startsWith("T:"))  {
          tupleCount[Integer.valueOf(line.substring(2, line.indexOf(':', 2)))]++;
        }
      }
    }
    Assert.assertTrue("begin window should exist", beginWindowExists);
    Assert.assertTrue("end window should exist", endWindowExists);
    int sum = 0;
    for (int i = 0; i < numPorts; i++) {
      Assert.assertTrue("tuple exists for port " + i, tupleCount[i] > 0);
      sum += tupleCount[i];
    }
    Assert.assertTrue("total tuple count >= " + testTupleCount, sum >= testTupleCount);
  }

}