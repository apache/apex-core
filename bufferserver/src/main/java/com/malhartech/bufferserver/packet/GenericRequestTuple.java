/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

import static com.malhartech.bufferserver.packet.Tuple.CLASSIC_VERSION;
import static com.malhartech.bufferserver.packet.Tuple.writeString;
import com.malhartech.bufferserver.util.Codec;
import java.util.Arrays;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class GenericRequestTuple extends RequestTuple
{
  public static final String EMPTY_STRING = new String();
  public String version;
  protected String identifier;
  protected int baseSeconds;
  protected int windowId;

  public GenericRequestTuple(byte[] buffer, int offset, int length)
  {
    super(buffer, offset, length);
  }

  @Override
  public boolean isValid()
  {
    return valid;
  }

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
    if (baseSeconds != Integer.MIN_VALUE) {
      while (buffer[dataOffset++] < 0) {
      }
    }
    else {
      return;
    }

    windowId = readVarInt(dataOffset, limit);
    if (windowId >= 0) {
      while (buffer[dataOffset++] < 0) {
      }
    }
    else {
      return;
    }
    valid = true;
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

  @Override
  public String getVersion()
  {
    return version;
  }

  @Override
  public String getIdentifier()
  {
    return identifier;
  }

  public static byte[] getSerializedRequest(String version, String identifier, long startingWindowId, byte type)
  {
    byte[] array = new byte[4096];
    int offset = 0;

    /* write the type */
    array[offset++] = type;

    /* write the version */
    if (version == null) {
      version = CLASSIC_VERSION;
    }
    offset = writeString(version, array, offset);

    /* write the identifer */
    offset = writeString(identifier, array, offset);

    /* write the baseSeconds */
    int baseSeconds = (int)(startingWindowId >> 32);
    offset = Codec.writeRawVarint32(baseSeconds, array, offset);

    /* write the windowId */
    int windowId = (int)startingWindowId;
    offset = Codec.writeRawVarint32(windowId, array, offset);

    return Arrays.copyOfRange(array, 0, offset);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" + "version=" + version + ", identifier=" + identifier + ", baseSeconds=" + baseSeconds + ", windowId=" + windowId + '}';
  }

}
