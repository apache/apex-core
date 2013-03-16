/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.bufferserver.packet;

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
    if (baseSeconds > 0) {
      while (buffer[dataOffset++] < 0) {
      }
    }
    else {
      return;
    }
    windowId = readVarInt(dataOffset, limit);
    if (windowId > 0) {
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
  public int getPartition()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getDataOffset()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getBaseSeconds()
  {
    return baseSeconds;
  }

  @Override
  public int getWindowWidth()
  {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

}
