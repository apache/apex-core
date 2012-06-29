package com.malhartech.stram;

import com.malhartech.stram.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import java.io.IOException;
import java.util.Collections;
import junit.framework.Assert;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Test;

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
    StramMiniClusterTest.TestDNode d = new StramMiniClusterTest.TestDNode();
    d.setTupleCounts("100, 100, 1000");
    Assert.assertEquals("100,100,1000", d.getTupleCounts());

    Assert.assertEquals("heartbeat1", 100, d.getResetCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat2", 100, d.getResetCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat3", 1000, d.getResetCounters().tuplesProcessed);
    Assert.assertEquals("heartbeat4", 100, d.getResetCounters().tuplesProcessed);
    
  }
  
  
}
