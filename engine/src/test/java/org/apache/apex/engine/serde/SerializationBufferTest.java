package org.apache.apex.engine.serde;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Output;


public class SerializationBufferTest
{
  private static final Logger logger = LoggerFactory.getLogger(SerializationBufferTest.class);

  @Test
  public void testWriteVarIntPerformance()
  {
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();
    long beginTime = System.currentTimeMillis();
    for (int i = 0; i < 10000000; ++i) {
      output.writeVarInt(i, true);
    }
    logger.info("Spent time on writeVarInt: {}", System.currentTimeMillis() - beginTime);
  }

  @Test
  public void testOutputStreamOverflow()
  {
    try {
      Output output = new Output(1024);
      output.write(new byte[2048]);
      Assert.assertFalse("Except exception", true);
    } catch (KryoException e) {
      //ignore
    }
  }

  @Test
  public void testSerializationBuffer()
  {
    try {
      SerializationBuffer output = SerializationBuffer.READ_BUFFER;
      output.write(new byte[Block.DEFAULT_BLOCK_SIZE + 10]);
    } catch (KryoException e) {
      Assert.assertFalse("Not except has exception", true);
    }
  }
}
