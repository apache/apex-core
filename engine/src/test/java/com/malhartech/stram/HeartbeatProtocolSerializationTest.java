package com.malhartech.stram;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Test;

import com.malhartech.dag.InputAdapter;
import com.malhartech.dag.NodeContext;
import com.malhartech.dag.StreamConfiguration;
import com.malhartech.dag.StreamContext;
import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;

public class HeartbeatProtocolSerializationTest {

  @Test
  public void testLoadFromPropertiesFile() throws IOException {

    StreamingNodeContext snc = new StreamingNodeContext();
    snc.setLogicalId("node1");
    
    StreamingContainerContext scc = new StreamingContainerContext();
    scc.setNodes(Collections.singletonList(snc));

    DataOutputByteBuffer out = new DataOutputByteBuffer();
    scc.write(out);
    
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(out.getData());
    
    StreamingContainerContext clone = new StreamingContainerContext();
    clone.readFields(in);
    
    Assert.assertNotNull(clone.getNodes());
    Assert.assertEquals(1, clone.getNodes().size());
    Assert.assertEquals("node1", clone.getNodes().get(0).getLogicalId());
    
  }

  @Test
  public void testMiniClusterTestNode() {
    StramMiniClusterTest.TestDNode d = new StramMiniClusterTest.TestDNode(new NodeContext("1"));
    d.setTupleCounts("100, 100, 1000");
    Assert.assertEquals("100,100,1000", d.getTupleCounts());

    Assert.assertEquals("heartbeat1", 100, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat2", 100, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat3", 1000, d.resetHeartbeatCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat4", 100, d.resetHeartbeatCounters().tuplesProcessed);
    
  }
  
  @Test
  public void testWindowGen() throws Exception {
    final AtomicLong currentWindow = new AtomicLong();
    final AtomicInteger beginWindowCount = new AtomicInteger();
    final AtomicInteger endWindowCount = new AtomicInteger();
    
    final AtomicLong windowXor = new AtomicLong();
    
    InputAdapter ia = new InputAdapter() {
      @Override
      public void teardown() {
      }
      @Override
      public void setup(StreamConfiguration config) {
      }
      @Override
      public void setContext(StreamContext context) {
      }
      @Override
      public void endWindow(long timemillis) {
        endWindowCount.incrementAndGet();
        windowXor.set(windowXor.get() ^ timemillis);
        System.out.println("end  : " + timemillis + " (" + System.currentTimeMillis() + ")");
      }
      @Override
      public void beginWindow(long timemillis) {
        currentWindow.set(timemillis);
        beginWindowCount.incrementAndGet();
        windowXor.set(windowXor.get() ^ timemillis);
        System.out.println("begin: " + timemillis + " (" + System.currentTimeMillis() + ")");
      }
    };
    
    long startTime = System.currentTimeMillis();
    startTime = startTime - 1000;
    long intervalMillis = 200;
    WindowGenerator wg = new WindowGenerator(Collections.singletonList(ia), startTime, intervalMillis);
    wg.start();
    
    Thread.sleep(300);
    
    wg.stop();
    System.out.println("completed windows: " + endWindowCount.get());
    Assert.assertEquals("only last window open", currentWindow.get(), windowXor.get());
    
    long expectedCnt = (System.currentTimeMillis() - startTime) / intervalMillis;
    Assert.assertEquals("begin window count", expectedCnt+1, beginWindowCount.get());
    Assert.assertEquals("end window count", expectedCnt, endWindowCount.get());
    
  }
  
}
