/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.malhartech.bufferserver.Buffer.Message;
import com.malhartech.bufferserver.Buffer.Message.MessageType;
import com.malhartech.bufferserver.util.SerializedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ProtobufDataInspector implements DataIntrospector
{
  private static final Logger logger = LoggerFactory.getLogger(ProtobufDataInspector.class);
  int previousOffset = -1;
  Message previousMessage;

  /**
   *
   * @param data
   */
  private void readyMessage(SerializedData data)
  {
    if (data.offset != previousOffset) {
      try {
        int size = data.size - data.dataOffset + data.offset;
        if (size < BasicDataMinLength) {
          //logger.debug("since the data is smaller than BasicDataMinLength, assuming it's missing");
          previousMessage = null;
        }
        else {
          if (data.bytes.length < data.dataOffset + size) {
            logger.debug("strange");
          }
          previousMessage = Message.newBuilder().mergeFrom(data.bytes, data.dataOffset, size).build();
        }
      }
      catch (InvalidProtocolBufferException ipbe) {
        logger.debug(ipbe.getLocalizedMessage());
        previousMessage = null;
      }

      previousOffset = data.offset;
    }
  }

  /**
   *
   * @param data
   * @return MessageType
   */
  @Override
  public final MessageType getType(SerializedData data)
  {
    readyMessage(data);
    return previousMessage == null ? Message.MessageType.NO_MESSAGE : previousMessage.getType();
  }

  /**
   *
   * @param data
   * @return long
   */
  @Override
  public final int getWindowId(SerializedData data)
  {
    readyMessage(data);

    int windowId;
    if (previousMessage == null) {
      windowId = 0;
    }
    else {
      switch (previousMessage.getType()) {
        case BEGIN_WINDOW:
          windowId = previousMessage.getBeginWindow().getWindowId();
          break;

        default:
          windowId = 0;
          break;
      }
    }

    return windowId;
  }

  /**
   *
   * @param data
   * @return Message
   */
  @Override
  public final Message getData(SerializedData data)
  {
    readyMessage(data);
    return previousMessage;
  }

  /**
   *
   * @param data
   */
  @Override
  public void wipeData(SerializedData data)
  {
    if (data.size + data.offset - data.dataOffset < BasicDataMinLength) {
      logger.debug("we do not need to wipe the SerializedData since it's smaller than min length");
      /* Arrays.fill(data.bytes, data.dataOffset, data.size + data.offset, (byte) 0); */
    }
    else {
      System.arraycopy(BasicData, 0, data.bytes, data.dataOffset, BasicDataMinLength);
    }
  }

  /**
   * Writes the NO_MESSAGE record if there is room or else returns the size of the data it wrote to the buffer.
   * @param bytes The array which contains the region to wipe out
   * @param offset The start offset of th region
   * @param length The size of the region in bytes
   */
  public static void wipeData(byte[] bytes, int offset, int length)
  {
    final int[] varIntCapacity = {
      (int)Math.pow(2, 7),
      (int)Math.pow(2, 14),
      (int)Math.pow(2, 21),
      (int)Math.pow(2, 28),
      (int)Math.pow(2, 31)
    };

    if (length >= BasicDataMinLength) {
      Message.Builder db = Message.newBuilder();
      int bytelen = length - BasicDataMinLength;
      if (bytelen == 0) {
        db.setType(MessageType.NO_MESSAGE);
      }
      else if (bytelen == ZeroByteArrayLength) {
        db.setType(MessageType.NO_MESSAGE);
        db.setNoMessagePadding(ByteString.EMPTY);
      }
      else if (bytelen > ZeroByteArrayLength) {
        db.setType(MessageType.NO_MESSAGE);
        bytelen -= ZeroByteArrayLength;
        // there has to be a better way to get the ByteString of any size without actually allocating!
        for (int i = 0; i < varIntCapacity.length; i++) {
          if (bytelen < varIntCapacity[i]) {
            db.setNoMessagePadding(ByteString.copyFrom(new byte[bytelen]));
            break;
          }
          bytelen--;
        }
      }
      else {
        db.setType(MessageType.NO_MESSAGE_ODD);
      }

      Message m = db.build();
      System.arraycopy(m.toByteArray(), 0, bytes, offset, m.getSerializedSize());
    }
  }

  /**
   * Here is a hope that Protobuf implementation is not very different than what small common sense would dictate.
   */
  private static final int BasicDataMinLength;
  private static final byte[] BasicData;
  private static final int ZeroByteArrayLength;

  static {
    Message.Builder db = Message.newBuilder();
    db.setType(MessageType.NO_MESSAGE);
    Message basic = db.build();
    BasicData = basic.toByteArray();
    BasicDataMinLength = basic.getSerializedSize();
    if (BasicData.length != BasicDataMinLength) {
      logger.debug("BasicDataMinLength({}) != BasicData.length({})", BasicDataMinLength, BasicData.length);
    }

    db.setNoMessagePadding(ByteString.EMPTY);
    basic = db.build();
    ZeroByteArrayLength = basic.getSerializedSize() - BasicDataMinLength;
    logger.debug("ZeroByteArrayLength = {}", ZeroByteArrayLength);
  }

  public int getBaseSeconds(SerializedData data)
  {
    readyMessage(data);

    int baseSeconds;
    if (previousMessage == null) {
      baseSeconds = 0;
    }
    else {
      switch (previousMessage.getType()) {
        case RESET_WINDOW:
          baseSeconds = previousMessage.getResetWindow().getBaseSeconds();
          break;

        default:
          baseSeconds = 0;
          break;
      }
    }

    return baseSeconds;
  }

}
