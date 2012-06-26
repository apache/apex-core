package com.malhar.stram.conf;

import java.io.IOException;
import java.util.Collections;

import junit.framework.Assert;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputByteBuffer;
import org.junit.Test;

import com.malhar.app.StreamingNodeUmbilicalProtocol.StreamingContainerContext;
import com.malhar.app.StreamingNodeUmbilicalProtocol.StreamingNodeContext;

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
  
  
}
