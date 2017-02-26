package com.datatorrent.stram.codec;

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.engine.serde.PartitionSerde;
import org.apache.apex.engine.serde.SerializationBuffer;

import com.datatorrent.bufferserver.packet.PayloadTuple;
import com.datatorrent.netlet.util.Slice;
import com.datatorrent.stram.codec.StatefulStreamCodec.DataStatePair;

public class CodecPerformanceTest
{
  private static final Logger logger = LoggerFactory.getLogger(CodecPerformanceTest.class);

  private DefaultStatefulStreamCodec codec = new DefaultStatefulStreamCodec();

  protected int loop = 1;
  private int numOfValues = 1000000;
  protected static String[] values = null;

  private Random random = new Random();
  private int valueLen = 1000;

  protected final int logPeriod = 3000000;

  public void initValues()
  {
    if (values != null) {
      return;
    }
    values = new String[numOfValues];

    //init chars
    char[] chars = new char[26 * 2 + 10];
    int i = 0;
    for (; i < 26; ++i) {
      chars[i] = (char)('A' + i);
    }
    for (; i < 52; ++i) {
      chars[i] = (char)('a' + i - 26);
    }
    for (; i < chars.length; ++i) {
      chars[i] = (char)('0' + i - 52);
    }

    char[] chars1 = new char[valueLen];
    for (i = 0; i < values.length; ++i) {
      for (int j = 0; j < valueLen; ++j) {
        chars1[j] = chars[random.nextInt(chars.length)];
      }
      values[i] = new String(chars1);
    }

    logger.info("initValues() done.");
  }

  protected boolean equals(DataStatePair dp1, DataStatePair dp2)
  {
    if (dp1 == null || dp2 == null) {
      return dp1 == dp2;
    }
    if (!equals(dp1.data, dp2.data)) {
      return false;
    }
    return equals(dp1.state, dp2.state);
  }

  protected boolean equals(Slice slice1, Slice slice2)
  {
    if (slice1 == null || slice2 == null) {
      return slice1 == slice2;
    }
    if (slice1.buffer == null || slice2.buffer == null) {
      return slice1.buffer == slice2.buffer;
    }
    if (slice1.length != slice2.length) {
      return false;
    }
    for (int i = 0; i < slice1.length; ++i) {
      if (slice1.buffer[slice1.offset + i] != slice2.buffer[slice2.offset + i]) {
        return false;
      }
    }
    return true;
  }

//  @Test
//  public void testCompareForInt()
//  {
//    coolDown();
//    testPartitionSerdeForInt();
//    coolDown();
//    testDataStatePairForInt();
//  }

  private final int maxIntValue = 100000;

  @Test
  public void testPartitionSerdeForInt()
  {
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    long startTime = System.currentTimeMillis();
    int count = 0;
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < maxIntValue; ++i) {
        if (count++ > 10000) {
          output.reset();
          count = 0;
        }
        serde.serialize(i, i, output);
      }
    }
    logger.info("spent times for PartitionSerde for int: {}", System.currentTimeMillis() - startTime);
  }

  @Test
  public void testDataStatePairForInt()
  {
    long startTime = System.currentTimeMillis();
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < maxIntValue; ++i) {
        DataStatePair dsp = codec.toDataStatePair(i);
        PayloadTuple.getSerializedTuple(i, dsp.data);
      }
    }
    logger.info("spent times for DataState for int: {}", System.currentTimeMillis() - startTime);
  }

//  @Test
//  public void testCompareForString()
//  {
//    testPartitionSerdeFunctional();
//    coolDown();
//    testDataStatePairForString();
//    coolDown();
//    testPartitionSerdeForString();
//  }

  @Test
  public void testPartitionSerdeFunctional()
  {
    initValues();
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    for (int i = 0; i < Math.min(values.length, 1000); i++) {
      int partition = codec.getPartition(values[i]);

      @SuppressWarnings("unchecked")
      DataStatePair dsp = codec.toDataStatePair(values[i]);
      byte[] array = PayloadTuple.getSerializedTuple(partition, dsp.data);

      Slice slice = serde.serialize(partition, values[i], output);
      byte[] array1 = new byte[slice.length];
      System.arraycopy(slice.buffer, slice.offset, array1, 0, array1.length);
      Assert.assertArrayEquals(array, array1);
    }
  }

  @Test
  public void testPartitionSerdeForString()
  {
    initValues();
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    long startTime = System.currentTimeMillis();
    long count = 0;
    resetLogRate();
    logRate(count);
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        serde.serialize(i, values[i], output);
        if (++count % logPeriod == 0) {
          logRate(count);
        }
        if (count % 1000 == 0) {
          output.reset();
        }
      }
    }
    logger.info("spent times for PartitionSerde for string: {}", System.currentTimeMillis() - startTime);
  }

  @Test
  public void testDataStatePairForString()
  {
    initValues();
    long startTime = System.currentTimeMillis();
    long count = 0;
    resetLogRate();
    logRate(count);
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < values.length; ++i) {
        DataStatePair dsp = codec.toDataStatePair(values[i]);
        PayloadTuple.getSerializedTuple(i, dsp.data);
        if (++count % logPeriod == 0) {
          logRate(count);
        }
      }
    }
    logger.info("spent times for DataState for string: {}", System.currentTimeMillis() - startTime);
  }


//  @Test
//  public void testCompareForSimpleTuple()
//  {
//    coolDown();
//    testPartitionSerdeForSimpleTuple();
//    coolDown();
//    testDataStatePairForSimpleTuple();
//  }

  static class SimpleTuple
  {
    SimpleTuple(int age, String name)
    {
      this.age = age;
      this.name = name;
    }

    int age;
    String name;
  }

  private SimpleTuple[] tuples;
  protected void initTuples()
  {
    if (tuples != null) {
      return;
    }
    initValues();
    tuples = new SimpleTuple[numOfValues];
    for (int i = 0; i < tuples.length; ++i) {
      tuples[i] = new SimpleTuple(i, values[i]);
    }
  }

  @Test
  public void testPartitionSerdeForSimpleTuple()
  {
    initTuples();
    resetLogRate();

    logger.info("Test PartitionSerde for SimpleTuples...");
    PartitionSerde serde = new PartitionSerde();
    SerializationBuffer output = SerializationBuffer.READ_BUFFER;
    output.reset();

    long startTime = System.currentTimeMillis();
    long count = 0;
    logRate(count);
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < tuples.length; ++i) {
        serde.serialize(i, tuples[i], output);

        if (++count % logPeriod == 0) {
          logRate(count);
        }
        if (count % 1000 == 0) {
          output.reset();
        }
      }
    }
    logger.info("spent times for PartitionSerde for SimpleTuples: {}", System.currentTimeMillis() - startTime);
  }

  @Test
  public void testDataStatePairForSimpleTuple()
  {
    initTuples();
    resetLogRate();

    long startTime = System.currentTimeMillis();
    long count = 0;
    logRate(count);
    for (int j = 0; j < loop; ++j) {
      for (int i = 0; i < tuples.length; ++i) {
        DataStatePair dsp = codec.toDataStatePair(tuples[i]);
        PayloadTuple.getSerializedTuple(i, dsp.data);
        if (++count % logPeriod == 0) {
          logRate(count);
        }
      }
    }
    logger.info("spent times for DataState for SimpleTuples: {}", System.currentTimeMillis() - startTime);
  }


  /**
   * Cool Down to make sure following test case run with fair condition
   */
  private static void coolDown()
  {
    try {
      Thread.sleep(2000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private long beginTime = 0;
  private long lastLogTime = 0;
  private long lastCount = 0;

  protected void resetLogRate()
  {
    beginTime = 0;
    lastLogTime = 0;
    lastCount = 0;
  }

  protected void logRate(long count)
  {
    long now = System.currentTimeMillis();
    if (lastLogTime == 0) {
      lastLogTime = now;
    }
    if (beginTime == 0) {
      beginTime = now;
    }
    if (now > lastLogTime) {
      logger.info("Time: " + (now - lastLogTime) + "; period rate: " + (count - lastCount) / (now - lastLogTime)
          + "; total rate: " + count / (now - beginTime));
      lastLogTime = now;
      lastCount = count;
    }
  }
}
