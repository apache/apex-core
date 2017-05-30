/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.debug;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.AsyncFSStorageAgent;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.debug.TupleRecorder.PortInfo;
import com.datatorrent.stram.engine.GenericTestOperator;
import com.datatorrent.stram.engine.StreamingContainer;
import com.datatorrent.stram.engine.TestGeneratorInputOperator;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.plan.physical.PTOperator;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;
import com.datatorrent.stram.util.FSPartFileCollection;

/**
 *
 */
public class TupleRecorderTest
{
  private final String classname;

  @Before
  public void setup() throws IOException
  {
    StreamingContainer.eventloop.start();
  }

  @After
  public void teardown()
  {
    StreamingContainer.eventloop.stop();
  }

  public TupleRecorderTest()
  {
    classname = "com.datatorrent.stram.debug.TupleRecorderCollection";
  }

  public TupleRecorder getTupleRecorder(final StramLocalCluster localCluster, final PTOperator op)
  {
    TupleRecorderCollection instance = (TupleRecorderCollection)localCluster.getContainer(op).getInstance(classname);
    return instance.getTupleRecorder(op.getId(), null);
  }

  public class Tuple
  {
    public String key;
    public String value;
  }

  @Test
  public void testRecorder() throws IOException
  {
    try (FileSystem fs = new LocalFileSystem()) {
      TupleRecorder recorder = new TupleRecorder(null, "application_test_id_1");
      recorder.getStorage().setBytesPerPartFile(4096);
      recorder.getStorage().setLocalMode(true);
      recorder.getStorage().setBasePath("file://" + testWorkDir.getAbsolutePath() + "/recordings");

      recorder.addInputPortInfo("ip1", "str1");
      recorder.addInputPortInfo("ip2", "str2");
      recorder.addInputPortInfo("ip3", "str3");
      recorder.addOutputPortInfo("op1", "str4");
      recorder.setup(null, null);

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

      fs.initialize((new Path(recorder.getStorage().getBasePath()).toUri()), new Configuration());
      Path path;
      String line;

      path = new Path(recorder.getStorage().getBasePath(), FSPartFileCollection.INDEX_FILE);
      try (FSDataInputStream is = fs.open(path);
          BufferedReader br = new BufferedReader(new InputStreamReader(is))) {

        line = br.readLine();
        //    Assert.assertEquals("check index", "B:1000:T:0:part0.txt", line);
        Assert.assertTrue("check index", line
            .matches("F:part0.txt:\\d+-\\d+:4:T:1000-1000:33:\\{\"3\":\"1\",\"1\":\"1\",\"0\":\"1\",\"2\":\"1\"\\}"));
      }
      path = new Path(recorder.getStorage().getBasePath(), FSPartFileCollection.META_FILE);
      try (FSDataInputStream is = fs.open(path);
          BufferedReader br = new BufferedReader(new InputStreamReader(is))) {

        ObjectMapper mapper = new ObjectMapper();
        line = br.readLine();
        Assert.assertEquals("check version", "1.2", line);
        br.readLine(); // RecordInfo
        //RecordInfo ri = mapper.readValue(line, RecordInfo.class);
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
        Assert.assertEquals("port size", 4, recorder.getPortInfoMap().size());
        //line = br.readLine();
      }
      path = new Path(recorder.getStorage().getBasePath(), "part0.txt");
      try (FSDataInputStream is = fs.open(path);
          BufferedReader br = new BufferedReader(new InputStreamReader(is))) {

        line = br.readLine();
        Assert.assertTrue("check part0", line.startsWith("B:"));
        Assert.assertTrue("check part0", line.endsWith(":1000"));

        line = br.readLine();
        Assert.assertTrue("check part0 1", line.startsWith("T:"));
        Assert.assertTrue("check part0 1", line.endsWith(":0:30:{\"key\":\"speed\",\"value\":\"5m/h\"}"));

        line = br.readLine();
        Assert.assertTrue("check part0 2", line.startsWith("T:"));
        Assert.assertTrue("check part0 2", line.endsWith(":2:30:{\"key\":\"speed\",\"value\":\"4m/h\"}"));

        line = br.readLine();
        Assert.assertTrue("check part0 3", line.startsWith("T:"));
        Assert.assertTrue("check part0 3", line.endsWith(":1:30:{\"key\":\"speed\",\"value\":\"6m/h\"}"));

        line = br.readLine();
        Assert.assertTrue("check part0 4", line.startsWith("T:"));
        Assert.assertTrue("check part0 4", line.endsWith(":3:30:{\"key\":\"speed\",\"value\":\"2m/h\"}"));

        line = br.readLine();
        Assert.assertTrue("check part0 5", line.startsWith("E:"));
        Assert.assertTrue("check part0 5", line.endsWith(":1000"));
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static final File testWorkDir = new File("target", TupleRecorderTest.class.getName());
  private static final long testTupleCount = 10;

  @Test
  public void testRecordingFlow() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    dag.setAttribute(Context.OperatorContext.STORAGE_AGENT, new AsyncFSStorageAgent(testWorkDir.getAbsolutePath(), null));

    dag.getAttributes().put(LogicalPlan.APPLICATION_PATH, "file://" + testWorkDir.getAbsolutePath());
    dag.getAttributes().put(LogicalPlan.TUPLE_RECORDING_PART_FILE_SIZE, 1024);  // 1KB per part

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

    testRecordingOnOperator(localCluster, ptOp2);

    final PTOperator ptOp1 = localCluster.findByLogicalNode(dag.getMeta(op1));
    StramTestSupport.waitForActivation(localCluster, ptOp1);

    testRecordingOnOperator(localCluster, ptOp1);

    localCluster.shutdown();
  }

  private void testRecordingOnOperator(final StramLocalCluster localCluster, final PTOperator op) throws Exception
  {
    String id = "xyz";
    localCluster.getStreamingContainerManager().startRecording(id, op.getId(), null, 0);

    WaitCondition c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        return null != getTupleRecorder(localCluster, op);
      }

    };
    Assert.assertTrue("Should get a tuple recorder within 10 seconds", StramTestSupport.awaitCompletion(c, 10000));
    final TupleRecorder tupleRecorder = getTupleRecorder(localCluster, op);
    long startTime = tupleRecorder.getStartTime();
    String line;
    File dir = new File(testWorkDir, "recordings/" + op.getId() + "/" + id);
    File file;

    file = new File(dir, FSPartFileCollection.META_FILE);
    Assert.assertTrue("meta file should exist", file.exists());
    int numPorts = tupleRecorder.getSinkMap().size();

    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      line = br.readLine();
      Assert.assertEquals("version should be 1.2", "1.2", line);
      line = br.readLine();
      JSONObject json = new JSONObject(line);
      Assert.assertEquals("Start time verification", startTime, json.getLong("startTime"));
      Assert.assertTrue(numPorts > 0);

      for (int i = 0; i < numPorts; i++) {
        line = br.readLine();
        Assert.assertTrue("should contain name, streamName, type and id", line != null && line
            .contains("\"name\"") && line.contains("\"streamName\"") && line.contains("\"type\"") && line
            .contains("\"id\""));
      }
    }

    c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        return (tupleRecorder.getTotalTupleCount() >= testTupleCount);
      }

    };

    Assert.assertTrue("Should record more than " + testTupleCount + " tuples within 15 seconds", StramTestSupport.awaitCompletion(c, 15000));

    localCluster.getStreamingContainerManager().stopRecording(op.getId(), null);
    c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        TupleRecorder tupleRecorder = getTupleRecorder(localCluster, op);
        return (tupleRecorder == null);
      }

    };
    Assert.assertTrue("Tuple recorder shouldn't exist any more after stopping", StramTestSupport.awaitCompletion(c, 5000));

    file = new File(dir, FSPartFileCollection.INDEX_FILE);
    Assert.assertTrue("index file should exist", file.exists());

    ArrayList<String> partFiles = new ArrayList<>();
    int indexCount = 0;
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      while ((line = br.readLine()) != null) {
        String partFile = "part" + indexCount + ".txt";
        if (line.startsWith("F:" + partFile + ":")) {
          partFiles.add(partFile);
          indexCount++;
        } else if (line.startsWith("E")) {
          Assert.assertEquals("index file should end after E line", br.readLine(), null);
          break;
        } else {
          Assert.fail("index file line is not starting with F or E");
        }
      }
    }

    int[] tupleCount = new int[numPorts];
    boolean beginWindowExists = false;
    boolean endWindowExists = false;

    for (String partFile : partFiles) {
      file = new File(dir, partFile);
      if (!partFile.equals(partFiles.get(partFiles.size() - 1))) {
        Assert.assertTrue(partFile + " should be greater than 1KB", file.length() >= 1024);
      }
      Assert.assertTrue(partFile + " should exist", file.exists());
      try (BufferedReader br = new BufferedReader(new FileReader(file))) {
        while ((line = br.readLine()) != null) {
          if (line.startsWith("B:")) {
            beginWindowExists = true;
          } else if (line.startsWith("E:")) {
            endWindowExists = true;
          } else if (line.startsWith("T:")) {
            String[] parts = line.split(":");
            tupleCount[Integer.valueOf(parts[2])]++;
          }
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

  private static final Logger logger = LoggerFactory.getLogger(TupleRecorderTest.class);
}
