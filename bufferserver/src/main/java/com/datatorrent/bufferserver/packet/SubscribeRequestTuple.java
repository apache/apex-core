/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.bufferserver.packet;

import com.datatorrent.bufferserver.util.Codec;

import java.util.Arrays;
import java.util.Collection;

/**
 * <p>SubscribeRequestTuple class.</p>
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 * @since 0.3.2
 */
public class SubscribeRequestTuple extends RequestTuple
{
  public static final String EMPTY_STRING = new String();
  private String version;
  private String identifier;
  private int baseSeconds;
  private int windowId;
  private String streamType;
  private String upstreamIdentifier;
  private int mask;
  private int[] partitions;

  @Override
  public void parse()
  {
    parsed = true;
    int dataOffset = offset + 1;
    int limit = offset + length;
    /*
     * read the version.
     */
    int idlen = readVarInt(dataOffset, limit);
    if (idlen > 0) {
      while (buffer[dataOffset++] < 0) {
      }
      version = new String(buffer, dataOffset, idlen);
      dataOffset += idlen;
    }
    else if (idlen == 0) {
      version = EMPTY_STRING;
      dataOffset++;
    }
    else {
      return;
    }
    /*
     * read the identifier.
     */
    idlen = readVarInt(dataOffset, limit);
    if (idlen > 0) {
      while (buffer[dataOffset++] < 0) {
      }
      identifier = new String(buffer, dataOffset, idlen);
      dataOffset += idlen;
    }
    else if (idlen == 0) {
      identifier = EMPTY_STRING;
      dataOffset++;
    }
    else {
      return;
    }

    baseSeconds = readVarInt(dataOffset, limit);
    if (getBaseSeconds() != Integer.MIN_VALUE) {
      while (buffer[dataOffset++] < 0) {
      }
    }
    else {
      return;
    }

    windowId = readVarInt(dataOffset, limit);
    if (windowId != Integer.MIN_VALUE) {
      while (buffer[dataOffset++] < 0) {
      }
    }
    else {
      return;
    }
    /*
     * read the type
     */
    idlen = readVarInt(dataOffset, limit);
    if (idlen > 0) {
      while (buffer[dataOffset++] < 0) {
      }
      streamType = new String(buffer, dataOffset, idlen);
      dataOffset += idlen;
    }
    else if (idlen == 0) {
      streamType = EMPTY_STRING;
      dataOffset++;
    }
    else {
      return;
    }
    /*
     * read the upstream identifier
     */
    idlen = readVarInt(dataOffset, limit);
    if (idlen > 0) {
      while (buffer[dataOffset++] < 0) {
      }
      upstreamIdentifier = new String(buffer, dataOffset, idlen);
      dataOffset += idlen;
    }
    else if (idlen == 0) {
      upstreamIdentifier = EMPTY_STRING;
      dataOffset++;
    }
    else {
      return;
    }
    /*
     * read the partition count
     */
    int count = readVarInt(dataOffset, limit);
    if (count > 0) {
      while (buffer[dataOffset++] < 0) {
      }
      mask = readVarInt(dataOffset, limit);
      if (getMask() > 0) {
        while (buffer[dataOffset++] < 0) {
        }
      }
      else {
        /* mask cannot be zero */
        return;
      }
      partitions = new int[count];
      for (int i = 0; i < count; i++) {
        partitions[i] = readVarInt(dataOffset, limit);
        if (getPartitions()[i] == -1) {
          return;
        }
        else {
          while (buffer[dataOffset++] < 0) {
          }
        }
      }
    }

    valid = true;
  }

  public boolean isParsed()
  {
    return parsed;
  }

  public String getStreamType()
  {
    return streamType;
  }

  public SubscribeRequestTuple(byte[] array, int offset, int length)
  {
    super(array, offset, length);
  }

  @Override
  public int getWindowId()
  {
    return windowId;
  }

  @Override
  public int getBaseSeconds()
  {
    return baseSeconds;
  }

  /**
   * @return the version
   */
  @Override
  public String getVersion()
  {
    return version;
  }

  /**
   * @return the identifier
   */
  @Override
  public String getIdentifier()
  {
    return identifier;
  }

  /**
   * @return the upstreamIdentifier
   */
  public String getUpstreamIdentifier()
  {
    return upstreamIdentifier;
  }

  /**
   * @return the mask
   */
  public int getMask()
  {
    return mask;
  }

  /**
   * @return the partitions
   */
  @SuppressWarnings(value = "ReturnOfCollectionOrArrayField")
  public int[] getPartitions()
  {
    return partitions;
  }

  public static byte[] getSerializedRequest(
          String version,
          String id,
          String down_type,
          String upstream_id,
          int mask,
          Collection<Integer> partitions,
          long startingWindowId)
  {
    byte[] array = new byte[4096];
    int offset = 0;

    /* write the type */
    array[offset++] = MessageType.SUBSCRIBER_REQUEST_VALUE;

    /* write the version */
    if (version == null) {
      version = CLASSIC_VERSION;
    }
    offset = Tuple.writeString(version, array, offset);

    /* write the identifier */
    offset = Tuple.writeString(id, array, offset);

    /* write the baseSeconds */
    int baseSeconds = (int)(startingWindowId >> 32);
    offset = Codec.writeRawVarint32(baseSeconds, array, offset);

    /* write the windowId */
    int windowId = (int)startingWindowId;
    offset = Codec.writeRawVarint32(windowId, array, offset);

    /* write the type */
    offset = Tuple.writeString(down_type, array, offset);

    /* write upstream identifier */
    offset = Tuple.writeString(upstream_id, array, offset);

    /* write the partitions */
    if (partitions == null || partitions.isEmpty()) {
      offset = Codec.writeRawVarint32(0, array, offset);
    }
    else {
      offset = Codec.writeRawVarint32(partitions.size(), array, offset);
      offset = Codec.writeRawVarint32(mask, array, offset);
      for (int i : partitions) {
        offset = Codec.writeRawVarint32(i, array, offset);
      }
    }

    return Arrays.copyOfRange(array, 0, offset);
  }

  @Override
  public String toString()
  {
    return "SubscribeRequestTuple{" + "version=" + version + ", identifier=" + identifier + ", baseSeconds=" + baseSeconds + ", windowId=" + windowId + ", type=" + streamType + ", upstreamIdentifier=" + upstreamIdentifier + ", mask=" + mask + ", partitions=" + (partitions == null? "null": Arrays.toString(partitions)) + '}';
  }

}
