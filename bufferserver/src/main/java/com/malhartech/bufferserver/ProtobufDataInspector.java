/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.malhartech.bufferserver.Buffer.Data;
import com.malhartech.bufferserver.Buffer.Data.DataType;
import com.malhartech.bufferserver.util.SerializedData;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ProtobufDataInspector implements DataIntrospector
{
  private static final Logger logger = LoggerFactory.getLogger(ProtobufDataInspector.class);
  final MessageLite prototype;
  SerializedData previousSerializedMessage;
  Data previousMessage;

  public ProtobufDataInspector(MessageLite prototype)
  {
    this.prototype = prototype;
  }

  private void readyMessage(SerializedData data)
  {
    if (data != previousSerializedMessage) {
      try {
        int size = data.size - data.dataOffset + data.offset;
        if (size < BasicDataMinLength) {
          logger.debug("since the data is smaller than BasicDataMinLength, assuming it's missing");
          previousMessage = null;
        }
        else {
          previousMessage = Data.newBuilder().mergeFrom(data.bytes, data.dataOffset, size).build();
        }
      }
      catch (InvalidProtocolBufferException ipbe) {
        logger.debug(ipbe.getLocalizedMessage());
        previousMessage = null;
      }

      previousSerializedMessage = data;
    }
  }

  public final DataType getType(SerializedData data)
  {
    readyMessage(data);
    return previousMessage == null ? Data.DataType.NO_DATA : previousMessage.getType();
  }

  public final long getWindowId(SerializedData data)
  {
    readyMessage(data);
    return previousMessage instanceof Data ? 0 : previousMessage.getWindowId();
  }

  public final ByteBuffer getPartitionedData(SerializedData data)
  {
    readyMessage(data);
    if (previousMessage == null) {
      return null;
    }
    else if (previousMessage.hasPartitioneddata()) {
      return previousMessage.getPartitioneddata().getPartition().asReadOnlyByteBuffer();
    }
    
    return null;
  }

  public void wipeData(SerializedData data)
  {
    if (data.size + data.offset - data.dataOffset > BasicDataMinLength) {
      System.arraycopy(BasicData, 0, data.bytes, data.dataOffset, BasicDataMinLength);
    }
    else {
      logger.debug("we do not need to wipe the SerializedData since it's smaller than min length");
      // Arrays.fill(data.bytes, data.dataOffset, data.size + data.offset, (byte) 0);
    }
  }
  /**
   * Here is a hope that Protobuf implementation is not very different than what small common sense would dictate.
   */
  private static final int BasicDataMinLength;
  private static final int BasicDataMaxLength;
  private static final byte[] BasicData;

  static {
    Data.Builder db = Data.newBuilder();
    db.setType(DataType.NO_DATA);
    db.setWindowId(0);
    Data basic = db.build();
    BasicData = basic.toByteArray();
    BasicDataMinLength = basic.getSerializedSize();
    if (BasicData.length != BasicDataMinLength) {
      logger.debug("BasicDataMinLength({}) != BasicData.length({})", BasicDataMinLength, BasicData.length);
    }

    db = Data.newBuilder();
    db.setType(DataType.NO_DATA); // i may need to change this if protobuf is compacting to smaller than 1 byte
    db.setWindowId(Long.MAX_VALUE);

    BasicDataMaxLength = db.build().getSerializedSize();
  }
}
